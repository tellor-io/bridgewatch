#!/usr/bin/env python3
"""
Valset Watcher

This component watches for ValidatorSetUpdated events from the data bridge contract
using the eth_getLogs RPC method. A "watcher" observes changes on EVM.
"""

import os
import json
import time
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from web3 import Web3
try:
    from web3.middleware import geth_poa_middleware
except ImportError:
    from web3.middleware.geth_poa import geth_poa_middleware
import csv
from config_manager import get_config_manager

# configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ValidatorSetUpdated event signature
# event ValidatorSetUpdated(uint256 _powerThreshold, uint256 _validatorTimestamp, bytes32 _validatorSetHash)
VALIDATOR_SET_UPDATED_TOPIC = Web3.keccak(text="ValidatorSetUpdated(uint256,uint256,bytes32)").hex()

# exponential backoff parameters
INITIAL_DELAY = 1  # seconds
MAX_DELAY = 60     # max seconds to wait
BACKOFF_MULTIPLIER = 2
MAX_RETRIES = 6    # number of exponential backoff attempts

class ValsetWatcher:
    def __init__(self, provider_url: str, bridge_address: str, output_prefix: str = "valset_updates", min_height: Optional[int] = None):
        self.w3 = Web3(Web3.HTTPProvider(provider_url))
        self.bridge_address = Web3.to_checksum_address(bridge_address)
        self.output_prefix = output_prefix
        self.min_height = min_height
        
        # get config manager and database
        try:
            self.config_manager = get_config_manager()
            self.db = self.config_manager.create_database_manager()
            self.data_dir = self.config_manager.get_valset_dir()
        except RuntimeError:
            # fallback to legacy mode - this shouldn't happen in database mode
            raise RuntimeError("Database mode requires configuration manager")
        
        # create data directory structure (for failure logs)
        os.makedirs(self.data_dir, exist_ok=True)
        
        # keep failure log file for debugging
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
        
        # initialize database schema if needed
        self.db.init_database()
    

    
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
        Load watcher state from database
        """
        try:
            state = self.db.get_component_state('valset_watcher')
            if state:
                # if min_height is specified and higher than saved state, use min_height
                if self.min_height is not None:
                    saved_block = state.get("last_processed_block", 0)
                    if self.min_height > saved_block:
                        logger.info(f"Using --min-height {self.min_height} instead of saved state block {saved_block}")
                        return {
                            "last_processed_block": self.min_height - 1,  # subtract 1 so we start from min_height
                            "total_events_found": 0
                        }
                
                logger.info(f"Loaded state from database. Last processed block: {state.get('last_processed_block', 'unknown')}")
                return state
            else:
                logger.info("No previous state found in database, starting fresh")
                # determine starting block for new state
                start_block = None
                
                if self.min_height is not None:
                    # use min_height if specified
                    start_block = self.min_height
                    logger.info(f"No previous state found, starting from --min-height {self.min_height}")
                else:
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
                    "total_events_found": 0
                }
        except Exception as e:
            logger.warning(f"Failed to load state from database: {e}")
            return {}
    
    def save_state(self, state: Dict[str, Any]):
        """
        Save watcher state to database
        """
        try:
            self.db.save_component_state('valset_watcher', state)
            logger.debug(f"Saved state to database: {state}")
        except Exception as e:
            logger.error(f"Failed to save state to database: {e}")
    
    def get_logs_with_retry(self, from_block: int, to_block: int, max_retries: int = MAX_RETRIES) -> List[Dict[str, Any]]:
        """
        Get event logs with retry logic
        """
        for attempt in range(max_retries):
            try:
                logger.debug(f"Calling eth.get_logs (attempt {attempt + 1}) for blocks {from_block}-{to_block}")
                
                # use Web3's built-in get_logs method instead of raw RPC
                logs = self.w3.eth.get_logs({
                    "fromBlock": from_block,
                    "toBlock": to_block,
                    "address": self.bridge_address,
                    "topics": [VALIDATOR_SET_UPDATED_TOPIC]  # remove the extra 0x prefix
                })
                
                logger.debug(f"Retrieved {len(logs)} logs for blocks {from_block}-{to_block}")
                return [dict(log) for log in logs] if logs else []
                
            except Exception as e:
                error_msg = str(e)
                logger.warning(f"eth_getLogs attempt {attempt + 1} failed for blocks {from_block}-{to_block}: {error_msg}")
                
                # check if it's a temporary service issue
                if "service temporarily unavailable" in error_msg.lower() or "timeout" in error_msg.lower():
                    if attempt < max_retries - 1:
                        logger.info(f"Retrying in {INITIAL_DELAY * BACKOFF_MULTIPLIER ** attempt} seconds...")
                        time.sleep(INITIAL_DELAY * BACKOFF_MULTIPLIER ** attempt)
                        continue
                
                # for the last attempt or non-retryable errors, log and re-raise
                if attempt == max_retries - 1:
                    logger.error(f"All {max_retries} attempts failed for blocks {from_block}-{to_block}")
                    raise e
        
        return []
    
    def decode_validator_set_updated_log(self, log: Dict[str, Any]) -> Dict[str, Any]:
        """
        Decode a ValidatorSetUpdated event log
        """
        # decode the event data
        # topics[0] is the event signature
        # data contains the non-indexed parameters: powerThreshold, validatorTimestamp, validatorSetHash
        
        data = log.get("data", "0x")
        
        # handle both string and bytes data formats
        if hasattr(data, 'hex'):  # HexBytes (check this first since HexBytes is a subclass of bytes)
            hex_str = data.hex()
            data = hex_str[2:] if hex_str.startswith("0x") else hex_str
        elif isinstance(data, bytes):
            # convert bytes to hex string
            data = data.hex()
        elif isinstance(data, str):
            # remove 0x prefix if present
            if data.startswith("0x"):
                data = data[2:]
        else:
            logger.warning(f"Unexpected data type for log data: {type(data)}")
            return {}
        
        # each parameter is 32 bytes (64 hex chars)
        if len(data) < 192:  # 3 * 64
            logger.warning(f"Insufficient data length for ValidatorSetUpdated event: {len(data)} chars")
            logger.debug(f"Raw data: 0x{data}")
            return {}
        
        # handle transaction hash (might be bytes or string)
        tx_hash = log.get("transactionHash", "")
        
        # check HexBytes first (before bytes, since HexBytes inherits from bytes)
        if hasattr(tx_hash, 'hex'):  # HexBytes from web3.py
            tx_hash = tx_hash.hex()  # already returns string with 0x prefix
        elif isinstance(tx_hash, bytes):
            tx_hash = "0x" + tx_hash.hex()
        elif isinstance(tx_hash, str) and not tx_hash.startswith("0x"):
            tx_hash = "0x" + tx_hash
                
        try:
            # decode uint256 powerThreshold (first 32 bytes)
            power_threshold = int(data[0:64], 16)
            
            # decode uint256 validatorTimestamp (second 32 bytes)
            validator_timestamp = int(data[64:128], 16)
            
            # decode bytes32 validatorSetHash (third 32 bytes)
            validator_set_hash = "0x" + data[128:192]
            
            result = {
                "timestamp": datetime.utcnow().isoformat(),
                "block_number": int(log.get("blockNumber", "0x0"), 16) if isinstance(log.get("blockNumber"), str) else log.get("blockNumber", 0),
                "tx_hash": tx_hash,
                "log_index": int(log.get("logIndex", "0x0"), 16) if isinstance(log.get("logIndex"), str) else log.get("logIndex", 0),
                "power_threshold": power_threshold,
                "validator_timestamp": validator_timestamp,
                "validator_set_hash": validator_set_hash
            }
            
            # debug logging to help verify the data
            logger.debug(f"Decoded ValidatorSetUpdated event:")
            logger.debug(f"  Block: {result['block_number']}")
            logger.debug(f"  Tx: {result['tx_hash']}")
            logger.debug(f"  Power Threshold: {result['power_threshold']}")
            logger.debug(f"  Validator Timestamp: {result['validator_timestamp']}")
            logger.debug(f"  Validator Set Hash: {result['validator_set_hash']}")
            logger.debug(f"  Raw data: 0x{data}")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to decode ValidatorSetUpdated event: {e}")
            logger.error(f"Raw data: 0x{data}")
            return {}
    
    def process_logs(self, logs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process event logs and decode ValidatorSetUpdated events
        """
        events = []
        
        for log in logs:
            # check if this is a ValidatorSetUpdated event
            topics = log.get("topics", [])
            if not topics:
                continue
                
            # compare the first topic (event signature)
            topic_hex = topics[0]
            if hasattr(topic_hex, 'hex'):
                topic_hex = topic_hex.hex()
            else:
                topic_hex = str(topic_hex)
                
            if topic_hex != VALIDATOR_SET_UPDATED_TOPIC:
                continue
                
            decoded_event = self.decode_validator_set_updated_log(log)
            if decoded_event:
                events.append(decoded_event)
        
        return events
    

    
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
    
    def write_event_data(self, event_data: Dict[str, Any]):
        """
        Write event data to database
        """
        try:
            # prepare data for database insertion
            data = {
                'timestamp': datetime.fromisoformat(event_data['timestamp'].replace('Z', '+00:00')),
                'block_number': event_data['block_number'],
                'tx_hash': event_data['tx_hash'],
                'log_index': event_data['log_index'],
                'power_threshold': event_data['power_threshold'],
                'validator_timestamp': event_data['validator_timestamp'],
                'validator_set_hash': event_data['validator_set_hash']
            }
            
            self.db.insert_evm_valset_update(data)
            logger.info(f"Saved ValidatorSetUpdated event from tx {event_data['tx_hash']} (block {event_data['block_number']})")
            
        except Exception as e:
            logger.error(f"Failed to write event data to database: {e}")
    
    def scan_block_batch_with_exponential_backoff(self, from_block: int, to_block: int, max_backoff: int = 600) -> int:
        """
        Scan a single batch of blocks with exponential backoff on failure
        """
        logger.info(f"Scanning blocks {from_block} to {to_block}")
        
        # first try the normal retry logic
        try:
            logs = self.get_logs_with_retry(from_block, to_block)
            
            if not logs:
                logger.debug(f"No ValidatorSetUpdated events found in blocks {from_block}-{to_block}")
                return 0
            
            # process logs to decode ValidatorSetUpdated events
            events = self.process_logs(logs)
            
            # write each event immediately
            for event_data in events:
                self.write_event_data(event_data)
            
            logger.info(f"Found {len(events)} ValidatorSetUpdated events in blocks {from_block}-{to_block}")
            return len(events)
            
        except Exception as first_error:
            logger.warning(f"Initial batch scan failed for blocks {from_block}-{to_block}: {first_error}")
            logger.info("Starting exponential backoff attempts...")
            
            # if initial attempt fails, try exponential backoff with single attempts
            backoff_delay = INITIAL_DELAY
            
            for attempt in range(MAX_RETRIES):
                logger.info(f"Exponential backoff attempt {attempt + 1}/{MAX_RETRIES}: waiting {min(backoff_delay, max_backoff)} seconds...")
                time.sleep(min(backoff_delay, max_backoff))
                
                try:
                    # make single attempt (no nested retries)
                    logs = self.get_logs_with_retry(from_block, to_block, max_retries=1)
                    
                    if not logs:
                        logger.debug(f"No ValidatorSetUpdated events found in blocks {from_block}-{to_block}")
                        return 0
                    
                    # process logs to decode ValidatorSetUpdated events
                    events = self.process_logs(logs)
                    
                    # write each event immediately
                    for event_data in events:
                        self.write_event_data(event_data)
                    
                    logger.info(f"Exponential backoff succeeded! Found {len(events)} ValidatorSetUpdated events in blocks {from_block}-{to_block}")
                    return len(events)
                    
                except Exception as e:
                    error_msg = str(e)
                    logger.warning(f"Exponential backoff attempt {attempt + 1} failed for blocks {from_block}-{to_block}: {error_msg}")
                    
                    # if this is the last attempt, log failure and raise
                    if attempt == MAX_RETRIES - 1:
                        logger.error(f"All {MAX_RETRIES} exponential backoff attempts failed for blocks {from_block}-{to_block}")
                        self.log_failed_batch(from_block, to_block, error_msg)
                        raise e
                    
                    # double the delay for next attempt
                    backoff_delay *= BACKOFF_MULTIPLIER
        
        return 0
    
    def scan_blocks(self, from_block: int, to_block: int, monitoring_interval: int = 300) -> int:
        """
        Scan a range of blocks for ValidatorSetUpdated events with batching
        """
        total_blocks = to_block - from_block + 1
        total_events = 0
        
        # calculate max backoff as 2x monitoring interval
        max_backoff = monitoring_interval * 2
        
        # determine if we're in catch-up mode
        is_catchup = total_blocks > 10000 # Assuming a large batch size for catch-up
        
        if is_catchup:
            logger.info(f"Catch-up mode: scanning {total_blocks} blocks in batches of {10000}")
            logger.info(f"Max backoff delay: {max_backoff} seconds (2x monitoring interval)")
        
        # process blocks in batches
        current_block = from_block
        batch_count = 0
        
        while current_block <= to_block:
            batch_end = min(current_block + 10000 - 1, to_block) # Use a fixed batch size for catch-up
            
            batch_count += 1
            logger.info(f"Processing batch {batch_count}: blocks {current_block}-{batch_end}")
            
            try:
                events_found = self.scan_block_batch_with_exponential_backoff(current_block, batch_end, max_backoff)
                total_events += events_found
                
                current_block = batch_end + 1
                
                catchup_delay = self.config_manager.get_catchup_delay()
                # add delay between batches during catch-up to avoid rate limiting
                if is_catchup and current_block <= to_block:
                    logger.info(f"Catch-up delay: waiting {catchup_delay} seconds before next batch...")
                    time.sleep(catchup_delay)
                    
            except Exception as e:
                logger.error(f"Failed to process batch {current_block}-{batch_end} after all retries: {e}")
                logger.warning(f"Skipping batch {current_block}-{batch_end} and continuing with next batch")
                # skip this batch and continue with the next one
                current_block = batch_end + 1
                continue
        
        logger.info(f"Completed scanning {total_blocks} blocks in {batch_count} batches, found {total_events} total events")
        return total_events
    
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
        events_found = self.scan_blocks(from_block, current_block, monitoring_interval)
        
        # update state
        state["last_processed_block"] = current_block
        state["total_events_found"] += events_found
        
        # save state
        self.save_state(state)
        
        logger.info(f"Monitoring cycle complete. Processed blocks {from_block}-{current_block}, found {events_found} events")
        logger.info(f"Total events found so far: {state['total_events_found']}")
        
        return state
    
    def run_continuous(self, interval_seconds: int = 300, block_buffer: int = 5):
        """
        Run continuous monitoring with specified interval
        """
        logger.info(f"Starting continuous monitoring (interval: {interval_seconds}s, block_buffer: {block_buffer})")
        logger.info(f"Output file: (database)")
        logger.info(f"State file: (database)")
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