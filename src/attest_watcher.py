#!/usr/bin/env python3
"""
Attest Watcher

This component watches for verifyOracleData calls to the data bridge contract
using debug_traceTransaction to extract method calls and parameters. 
A "watcher" observes changes on EVM while keeping "attestation" term for clarity.
"""

import json
import csv
import time
from typing import List, Dict, Any, Optional
from web3 import Web3
try:
    from web3.middleware import geth_poa_middleware
except ImportError:
    from web3.middleware.geth_poa import geth_poa_middleware
import logging
from datetime import datetime
import os

# configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# import config
from config import config, get_config_manager

# configuration from config module
WEB3_PROVIDER_URL = config.get_evm_rpc_url()
BRIDGE_CONTRACT_ADDRESS = config.get_bridge_address()
VERIFY_ORACLE_DATA_SELECTOR = config.get_function_selector('verifyOracleData')
MAX_RETRIES = config.get_max_retries()
RETRY_DELAY = config.get_retry_delay()
BLOCK_BATCH_SIZE = config.get_block_batch_size()
CATCHUP_DELAY = config.get_catchup_delay()

# other configuration
BATCH_SIZE = 100  # number of traces per batch

# Exponential backoff configuration for failed batches
INITIAL_BACKOFF = 5  # initial backoff delay in seconds
MAX_BACKOFF_ATTEMPTS = 5  # number of exponential backoff attempts

class AttestWatcher:
    def __init__(self, provider_url: str, bridge_address: str, output_prefix: str = "attestations"):
        self.w3 = Web3(Web3.HTTPProvider(provider_url))
        self.bridge_address = Web3.to_checksum_address(bridge_address)
        self.output_prefix = output_prefix
        
        # get config manager for directory paths
        try:
            config_manager = get_config_manager()
            self.data_dir = config_manager.get_oracle_dir()
        except RuntimeError:
            # fallback to legacy paths if in legacy mode
            self.data_dir = "data/oracle"
        
        # create data directory structure
        os.makedirs(self.data_dir, exist_ok=True)
        
        # output files in data directory
        self.state_file = f"{self.data_dir}/{output_prefix}_state.json"
        self.csv_file = f"{self.data_dir}/{output_prefix}.csv"
        self.failure_log_file = f"{self.data_dir}/{output_prefix}_failures.log"
        
        # add PoA middleware if needed
        try:
            self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        except Exception as e:
            logger.debug(f"Could not inject PoA middleware: {e}")
            pass
            
        # check connection
        try:
            connected = self.w3.is_connected
        except AttributeError:
            connected = self.w3.is_connected()
            
        if not connected:
            raise ConnectionError("Failed to connect to Web3 provider")
            
        logger.info(f"Connected to Web3. Latest block: {self.w3.eth.block_number}")
        
        # initialize CSV file with headers if it doesn't exist
        self.init_csv_file()
    
    def init_csv_file(self):
        """
        Initialize CSV file with headers if it doesn't exist
        """
        if not os.path.exists(self.csv_file):
            with open(self.csv_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'timestamp',
                    'block_number', 
                    'tx_hash',
                    'from_address',
                    'to_address',
                    'input_data',
                    'gas_used',
                    'trace_address'
                ])
    
    def find_block_by_timestamp(self, target_timestamp: int) -> int:
        """
        Find the block number closest to the target timestamp using binary search
        """
        current_block = self.w3.eth.block_number
        
        # estimate starting range (ethereum averages ~12-15 seconds per block)
        # 21 days = 1,814,400 seconds, so roughly 120k-150k blocks back
        estimated_blocks_back = int(1814400 / 13)  # use 13 seconds as average
        
        low = max(1, current_block - estimated_blocks_back - 10000)  # add buffer
        high = current_block
        
        logger.info(f"Searching for block at timestamp {target_timestamp} between blocks {low}-{high}")
        
        # binary search to find the block
        while high - low > 1:
            mid = (low + high) // 2
            try:
                block = self.w3.eth.get_block(mid)
                if block.timestamp < target_timestamp:
                    low = mid
                else:
                    high = mid
            except Exception as e:
                logger.warning(f"Could not get block {mid}: {e}")
                # fallback to estimation if binary search fails
                return max(1, current_block - estimated_blocks_back)
        
        result_block = low if abs(self.w3.eth.get_block(low).timestamp - target_timestamp) < abs(self.w3.eth.get_block(high).timestamp - target_timestamp) else high
        
        actual_timestamp = self.w3.eth.get_block(result_block).timestamp
        logger.info(f"Found block {result_block} with timestamp {actual_timestamp} (target: {target_timestamp})")
        
        return result_block
    
    def load_state(self) -> Dict[str, Any]:
        """
        Load the last processed state from file
        """
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load state file: {e}")
        
        # default state - start from 21 days ago
        logger.info("No previous state found, starting from 21 days ago")
        
        # calculate timestamp for 21 days ago
        import time as time_module
        days_ago_21 = 21 * 24 * 60 * 60  # 21 days in seconds
        target_timestamp = int(time_module.time()) - days_ago_21
        
        # find the block from 21 days ago
        start_block = self.find_block_by_timestamp(target_timestamp)
        
        return {
            "last_processed_block": start_block - 1,  # subtract 1 so we start from start_block
            "total_calls_found": 0
        }
    
    def save_state(self, state: Dict[str, Any]):
        """
        Save the current state to file
        """
        try:
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save state file: {e}")
    
    def trace_filter_calls_with_retry(self, from_block: int, to_block: int, after: int = 0, count: int = BATCH_SIZE, max_retries: int = MAX_RETRIES) -> List[Dict[str, Any]]:
        """
        Use trace_filter to find calls to the bridge contract with retry logic and pagination
        """
        filter_params = {
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "toAddress": [self.bridge_address],
            "after": after,
            "count": count
        }
        
        for attempt in range(max_retries):
            try:
                logger.debug(f"Calling trace_filter (attempt {attempt + 1}) with params: {filter_params}")
                traces = self.w3.manager.request_blocking("trace_filter", [filter_params])
                return traces or []
                
            except Exception as e:
                error_msg = str(e)
                logger.warning(f"trace_filter attempt {attempt + 1} failed for blocks {from_block}-{to_block}: {error_msg}")
                
                # check if it's a temporary service issue
                if "service temporarily unavailable" in error_msg.lower() or "timeout" in error_msg.lower():
                    if attempt < max_retries - 1:
                        logger.info(f"Retrying in {RETRY_DELAY} seconds...")
                        time.sleep(RETRY_DELAY)
                        continue
                
                # for the last attempt or non-retryable errors, log and re-raise
                if attempt == max_retries - 1:
                    logger.error(f"All {max_retries} attempts failed for blocks {from_block}-{to_block}")
                    raise e
        
        return []
    
    def trace_filter_calls(self, from_block: int, to_block: int) -> List[Dict[str, Any]]:
        """
        Use trace_filter to find calls to the bridge contract with pagination
        """
        all_traces = []
        after = 0
        
        while True:
            traces = self.trace_filter_calls_with_retry(from_block, to_block, after, BATCH_SIZE)
            
            if not traces:
                break
                
            all_traces.extend(traces)
            logger.debug(f"Retrieved {len(traces)} traces starting from offset {after}")
            
            # if we got fewer traces than batch size, we've reached the end
            if len(traces) < BATCH_SIZE:
                break
                
            # increment offset for next batch
            after += len(traces)
            
            # safety check to avoid infinite loops
            if after > 10000:  # trace_filter limit is typically 10,000
                logger.warning(f"Reached maximum trace limit for blocks {from_block}-{to_block}")
                break
        
        logger.info(f"Retrieved {len(all_traces)} total traces for blocks {from_block}-{to_block}")
        return all_traces
    
    def is_verify_oracle_data_call(self, input_data: str) -> bool:
        """
        Check if the input data corresponds to a verifyOracleData call
        """
        if not input_data or len(input_data) < 10:
            return False
            
        # get function selector (first 4 bytes)
        selector = input_data[:10].lower()
        
        # check if this matches the verifyOracleData function selector
        return selector == VERIFY_ORACLE_DATA_SELECTOR.lower()
    
    def process_traces(self, traces: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process trace results and extract verifyOracleData calls
        """
        oracle_calls = []
        
        for trace in traces:
            if trace.get("type") == "call":
                action = trace.get("action", {})
                input_data = action.get("input", "")
                
                # check if this is a verifyOracleData call
                if self.is_verify_oracle_data_call(input_data):
                    call_data = {
                        "timestamp": datetime.utcnow().isoformat(),
                        "block_number": int(trace.get("blockNumber", 0), 16) if isinstance(trace.get("blockNumber"), str) else trace.get("blockNumber", 0),
                        "tx_hash": trace.get("transactionHash", ""),
                        "from_address": action.get("from", ""),
                        "to_address": action.get("to", ""),
                        "input_data": input_data,
                        "gas_used": trace.get("result", {}).get("gasUsed", ""),
                        "trace_address": trace.get("traceAddress", [])
                    }
                    oracle_calls.append(call_data)
        
        return oracle_calls
    
    def write_to_csv(self, call_data: Dict[str, Any]):
        """
        Write call data to CSV file
        """
        try:
            with open(self.csv_file, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    call_data['timestamp'],
                    call_data['block_number'],
                    call_data['tx_hash'],
                    call_data['from_address'],
                    call_data['to_address'],
                    call_data['input_data'],
                    call_data['gas_used'],
                    json.dumps(call_data['trace_address'])  # serialize list as JSON string
                ])
                f.flush()
        except Exception as e:
            logger.error(f"Failed to write to CSV file: {e}")
    
    def log_failed_batch(self, from_block: int, to_block: int, error: str):
        """
        Log failed batch information to failure log file
        """
        try:
            failure_entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "from_block": from_block,
                "to_block": to_block,
                "error": str(error),
                "block_count": to_block - from_block + 1
            }
            
            with open(self.failure_log_file, 'a') as f:
                json.dump(failure_entry, f)
                f.write('\n')
                f.flush()
            
            logger.warning(f"Logged failed batch {from_block}-{to_block} to {self.failure_log_file}")
            
        except Exception as e:
            logger.error(f"Failed to write to failure log: {e}")
    
    def write_call_data(self, call_data: Dict[str, Any]):
        """
        Write call data to CSV file
        """
        self.write_to_csv(call_data)
        logger.info(f"Saved verifyOracleData call from tx {call_data['tx_hash']} (block {call_data['block_number']})")
    
    def scan_block_batch_with_exponential_backoff(self, from_block: int, to_block: int, max_backoff: int = 600) -> int:
        """
        Scan a single batch of blocks with exponential backoff on failure
        """
        logger.info(f"Scanning blocks {from_block} to {to_block}")
        
        # first try the normal retry logic
        try:
            traces = self.trace_filter_calls(from_block, to_block)
            
            if not traces:
                logger.debug(f"No traces found in blocks {from_block}-{to_block}")
                return 0
            
            # process traces to find verifyOracleData calls
            oracle_calls = self.process_traces(traces)
            
            # write each call immediately
            for call_data in oracle_calls:
                self.write_call_data(call_data)
            
            logger.info(f"Found {len(oracle_calls)} verifyOracleData calls in blocks {from_block}-{to_block}")
            return len(oracle_calls)
            
        except Exception as first_error:
            logger.warning(f"Initial batch scan failed for blocks {from_block}-{to_block}: {first_error}")
            logger.info("Starting exponential backoff attempts...")
            
            # if initial attempt fails, try exponential backoff with single attempts
            backoff_delay = INITIAL_BACKOFF
            
            for attempt in range(MAX_BACKOFF_ATTEMPTS):
                logger.info(f"Exponential backoff attempt {attempt + 1}/{MAX_BACKOFF_ATTEMPTS}: waiting {min(backoff_delay, max_backoff)} seconds...")
                time.sleep(min(backoff_delay, max_backoff))
                
                try:
                    # make single attempt (no nested retries)
                    traces = self.trace_filter_calls_with_retry(from_block, to_block, 0, BATCH_SIZE, max_retries=1)
                    
                    if not traces:
                        logger.debug(f"No traces found in blocks {from_block}-{to_block}")
                        return 0
                    
                    # process traces to find verifyOracleData calls
                    oracle_calls = self.process_traces(traces)
                    
                    # write each call immediately
                    for call_data in oracle_calls:
                        self.write_call_data(call_data)
                    
                    logger.info(f"Exponential backoff succeeded! Found {len(oracle_calls)} verifyOracleData calls in blocks {from_block}-{to_block}")
                    return len(oracle_calls)
                    
                except Exception as e:
                    error_msg = str(e)
                    logger.warning(f"Exponential backoff attempt {attempt + 1} failed for blocks {from_block}-{to_block}: {error_msg}")
                    
                    # if this is the last attempt, log failure and raise
                    if attempt == MAX_BACKOFF_ATTEMPTS - 1:
                        logger.error(f"All {MAX_BACKOFF_ATTEMPTS} exponential backoff attempts failed for blocks {from_block}-{to_block}")
                        self.log_failed_batch(from_block, to_block, error_msg)
                        raise e
                    
                    # double the delay for next attempt
                    backoff_delay *= 2
        
        return 0
    
    def scan_block_batch(self, from_block: int, to_block: int) -> int:
        """
        Scan a single batch of blocks for verifyOracleData calls
        """
        logger.info(f"Scanning blocks {from_block} to {to_block}")
        
        # get traces using trace_filter
        traces = self.trace_filter_calls(from_block, to_block)
        
        if not traces:
            logger.debug(f"No traces found in blocks {from_block}-{to_block}")
            return 0
        
        # process traces to find verifyOracleData calls
        oracle_calls = self.process_traces(traces)
        
        # write each call immediately
        for call_data in oracle_calls:
            self.write_call_data(call_data)
        
        logger.info(f"Found {len(oracle_calls)} verifyOracleData calls in blocks {from_block}-{to_block}")
        return len(oracle_calls)
    
    def scan_blocks(self, from_block: int, to_block: int, monitoring_interval: int = 300) -> int:
        """
        Scan a range of blocks for verifyOracleData calls with batching
        """
        total_blocks = to_block - from_block + 1
        total_calls = 0
        
        # calculate max backoff as 2x monitoring interval
        max_backoff = monitoring_interval * 2
        
        # determine if we're in catch-up mode
        is_catchup = total_blocks > BLOCK_BATCH_SIZE
        
        if is_catchup:
            logger.info(f"Catch-up mode: scanning {total_blocks} blocks in batches of {BLOCK_BATCH_SIZE}")
            logger.info(f"Max backoff delay: {max_backoff} seconds (2x monitoring interval)")
        
        # process blocks in batches
        current_block = from_block
        batch_count = 0
        
        while current_block <= to_block:
            batch_end = min(current_block + BLOCK_BATCH_SIZE - 1, to_block)
            
            batch_count += 1
            logger.info(f"Processing batch {batch_count}: blocks {current_block}-{batch_end}")
            
            try:
                calls_found = self.scan_block_batch_with_exponential_backoff(current_block, batch_end, max_backoff)
                total_calls += calls_found
                
                current_block = batch_end + 1
                
                # add delay between batches during catch-up to avoid rate limiting
                if is_catchup and current_block <= to_block:
                    logger.info(f"Catch-up delay: waiting {CATCHUP_DELAY} seconds before next batch...")
                    time.sleep(CATCHUP_DELAY)
                    
            except Exception as e:
                logger.error(f"Failed to process batch {current_block}-{batch_end} after all retries: {e}")
                logger.warning(f"Skipping batch {current_block}-{batch_end} and continuing with next batch")
                # skip this batch and continue with the next one
                current_block = batch_end + 1
                continue
        
        logger.info(f"Completed scanning {total_blocks} blocks in {batch_count} batches, found {total_calls} total calls")
        return total_calls
    
    def run_monitoring_cycle(self, block_buffer: int = 5) -> Dict[str, Any]:
        """
        Run one monitoring cycle
        """
        # load current state
        state = self.load_state()
        
        # get current block with buffer
        current_block = self.w3.eth.block_number - block_buffer
        from_block = state["last_processed_block"] + 1
        
        if from_block > current_block:
            logger.debug(f"No new blocks to process (last: {state['last_processed_block']}, current: {current_block})")
            return state
        
        # scan the block range (pass monitoring interval for backoff calculation)
        monitoring_interval = getattr(self, 'current_monitoring_interval', 300)
        calls_found = self.scan_blocks(from_block, current_block, monitoring_interval)
        
        # update state
        state["last_processed_block"] = current_block
        state["total_calls_found"] += calls_found
        
        # save state
        self.save_state(state)
        
        logger.info(f"Monitoring cycle complete. Processed blocks {from_block}-{current_block}, found {calls_found} calls")
        logger.info(f"Total calls found so far: {state['total_calls_found']}")
        
        return state
    
    def run_continuous(self, interval_seconds: int = 300, block_buffer: int = 5):
        """
        Run continuous monitoring with specified interval
        """
        logger.info(f"Starting continuous monitoring (interval: {interval_seconds}s, block_buffer: {block_buffer})")
        logger.info(f"Output file: {self.csv_file}")
        logger.info(f"State file: {self.state_file}")
        logger.info(f"Failure log: {self.failure_log_file}")
        
        try:
            while True:
                try:
                    # store interval for use in monitoring cycle
                    self.current_monitoring_interval = interval_seconds
                    self.run_monitoring_cycle(block_buffer)
                    time.sleep(interval_seconds)
                except KeyboardInterrupt:
                    logger.info("Monitoring stopped by user")
                    break
                except Exception as e:
                    logger.error(f"Error in monitoring cycle: {e}")
                    time.sleep(interval_seconds)  # wait before retrying
        except Exception as e:
            logger.error(f"Fatal error in continuous monitoring: {e}")
            raise