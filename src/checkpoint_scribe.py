#!/usr/bin/env python3
"""
Checkpoint Scribe

This component records validator checkpoint history from the Layer blockchain
using Layer's REST API endpoints. A "scribe" records the truth from Layer.
"""

import os
import json
import time
import requests
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from config_manager import get_config_manager

# configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# import config
from config import config, get_config_manager

# configuration from config module
MAX_RETRIES = config.get_max_retries()
RETRY_DELAY = config.get_retry_delay()
DEFAULT_LAYER_RPC_URL = config.get_layer_rpc_url()

# checkpoint polling configuration
CHECKPOINT_POLL_INTERVAL = 60  # seconds between checking for new checkpoints
CATCHUP_DELAY = config.get_catchup_delay()

# exponential backoff configuration for failed requests
INITIAL_BACKOFF = 5  # initial backoff delay in seconds
MAX_BACKOFF_ATTEMPTS = 5  # number of exponential backoff attempts

class CheckpointScribe:
    def __init__(self, layer_rpc_url: str, chain_id: str, output_prefix: str = "checkpoints"):
        self.layer_rpc_url = layer_rpc_url.rstrip('/')
        self.chain_id = chain_id
        self.output_prefix = output_prefix
        
        # get config manager and database
        try:
            self.config_manager = get_config_manager()
            self.db = self.config_manager.create_database_manager()
            self.data_dir = self.config_manager.get_layer_checkpoints_dir()
        except RuntimeError:
            # fallback to legacy mode - this shouldn't happen in database mode
            raise RuntimeError("Database mode requires configuration manager")
        
        # create data directory structure (for failure logs)
        os.makedirs(self.data_dir, exist_ok=True)
        
        # keep failure log files for debugging
        self.failure_log_file = f"{self.data_dir}/{chain_id}_{output_prefix}_failures.log"
        self.valset_failure_log_file = f"{self.data_dir}/{chain_id}_validator_sets_failures.log"
        
        # test connection
        self.test_connection()
        
        # initialize database schema if needed
        self.db.init_database()
    
    def test_connection(self):
        """
        Test connection to Layer RPC
        """
        try:
            url = f"{self.layer_rpc_url}/layer/bridge/get_current_validator_set_timestamp"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            current_timestamp = data.get('timestamp', 0)
            logger.info(f"Connected to Layer RPC. Current validator set timestamp: {current_timestamp}")
            
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Layer RPC at {self.layer_rpc_url}: {e}")
    
    def init_csv_files(self):
        """
        Initialize CSV files with headers if they don't exist
        """
        # initialize checkpoints CSV
        if not os.path.exists(self.csv_file):
            with open(self.csv_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'scrape_timestamp',
                    'validator_index',
                    'validator_timestamp',
                    'power_threshold',
                    'validator_set_hash',
                    'checkpoint'
                ])
        
        # initialize validator sets CSV
        if not os.path.exists(self.valset_csv_file):
            with open(self.valset_csv_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'scrape_timestamp',
                    'valset_timestamp',
                    'valset_checkpoint',
                    'ethereum_address',
                    'power'
                ])
    
    def load_state(self) -> Dict[str, Any]:
        """
        Load scribe state from database
        """
        try:
            state = self.db.get_component_state('checkpoint_scribe')
            if state:
                logger.info(f"Loaded state from database. Last index: {state.get('last_index', 'unknown')}")
                return state
            else:
                logger.info("No previous state found in database, starting from beginning")
                return {
                    "last_index": -1,
                    "last_timestamp": 0,
                    "total_checkpoints": 0,
                    "total_validator_sets": 0
                }
        except Exception as e:
            logger.warning(f"Failed to load state from database: {e}")
            return {
                "last_index": -1,
                "last_timestamp": 0,
                "total_checkpoints": 0,
                "total_validator_sets": 0
            }
    
    def save_state(self, state: Dict[str, Any]):
        """
        Save scribe state to database
        """
        try:
            self.db.save_component_state('checkpoint_scribe', state)
            logger.debug(f"Saved state to database: {state}")
        except Exception as e:
            logger.error(f"Failed to save state to database: {e}")
    
    def make_request_with_retry(self, url: str, max_retries: int = MAX_RETRIES) -> Optional[Dict[str, Any]]:
        """
        Make HTTP request with retry logic
        """
        for attempt in range(max_retries):
            try:
                logger.debug(f"Making request (attempt {attempt + 1}) to {url}")
                
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                logger.debug(f"Request successful: {url}")
                return data
                
            except Exception as e:
                error_msg = str(e)
                logger.warning(f"Request attempt {attempt + 1} failed for {url}: {error_msg}")
                
                # check if it's a temporary service issue
                if "timeout" in error_msg.lower() or "connection" in error_msg.lower():
                    if attempt < max_retries - 1:
                        logger.info(f"Retrying in {RETRY_DELAY} seconds...")
                        time.sleep(RETRY_DELAY)
                        continue
                
                # for the last attempt, log and return None
                if attempt == max_retries - 1:
                    logger.error(f"All {max_retries} attempts failed for {url}")
                    return None
        
        return None
    
    def get_current_validator_set_timestamp(self) -> Optional[int]:
        """
        Get the current validator set timestamp from Layer
        """
        url = f"{self.layer_rpc_url}/layer/bridge/get_current_validator_set_timestamp"
        data = self.make_request_with_retry(url)
        
        if data and 'timestamp' in data:
            return int(data['timestamp'])
        
        return None
    
    def get_validator_timestamp_by_index(self, index: int) -> Optional[int]:
        """
        Get validator timestamp by index
        """
        url = f"{self.layer_rpc_url}/layer/bridge/get_validator_timestamp_by_index/{index}"
        data = self.make_request_with_retry(url)
        
        if data and 'timestamp' in data:
            return int(data['timestamp'])
        
        return None
    
    def get_validator_checkpoint_params(self, timestamp: int) -> Optional[Dict[str, Any]]:
        """
        Get validator checkpoint parameters for a given timestamp
        """
        url = f"{self.layer_rpc_url}/layer/bridge/get_validator_checkpoint_params/{timestamp}"
        data = self.make_request_with_retry(url)
        
        if data:
            # extract and validate required fields
            try:
                result = {
                    'timestamp': int(data.get('timestamp', timestamp)),
                    'power_threshold': int(data.get('power_threshold', 0)),
                    'valset_hash': data.get('valset_hash', ''),
                    'checkpoint': data.get('checkpoint', '')
                }
                logger.debug(f"Retrieved checkpoint params for timestamp {timestamp}: {result}")
                return result
            except (ValueError, TypeError) as e:
                logger.error(f"Failed to parse checkpoint params for timestamp {timestamp}: {e}")
                return None
        
        return None
    
    def log_failed_checkpoint(self, timestamp: int, error: str):
        """
        Log failed checkpoint fetch to failure log file
        """
        try:
            failure_entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "validator_timestamp": timestamp,
                "error": str(error)
            }
            
            with open(self.failure_log_file, 'a') as f:
                json.dump(failure_entry, f)
                f.write('\n')
                f.flush()
            
            logger.warning(f"Logged failed checkpoint {timestamp} to {self.failure_log_file}")
            
        except Exception as e:
            logger.error(f"Failed to write to failure log: {e}")
    
    def write_checkpoint_data(self, index: int, checkpoint_data: Dict[str, Any]):
        """
        Write checkpoint data to database
        """
        try:
            # prepare checkpoint data for database insertion
            data = {
                'scrape_timestamp': datetime.utcnow(),
                'validator_index': index,
                'validator_timestamp': checkpoint_data['timestamp'],
                'power_threshold': checkpoint_data['power_threshold'],
                'validator_set_hash': checkpoint_data['valset_hash'],
                'checkpoint': checkpoint_data['checkpoint']
            }
            
            self.db.insert_layer_checkpoint(data)
            logger.info(f"Saved checkpoint data for index {index}, timestamp {checkpoint_data['timestamp']}")
            
            # fetch and save corresponding validator set
            logger.debug(f"Fetching validator set for checkpoint at timestamp {checkpoint_data['timestamp']}")
            valset_success = self.fetch_validator_set_with_retry(
                checkpoint_data['timestamp'], 
                checkpoint_data['checkpoint']
            )
            
            if valset_success:
                logger.debug(f"Successfully saved validator set for timestamp {checkpoint_data['timestamp']}")
                return True
            else:
                logger.warning(f"Failed to save validator set for timestamp {checkpoint_data['timestamp']}")
                return False
            
        except Exception as e:
            logger.error(f"Failed to write checkpoint data to database: {e}")
            return False
    
    def get_validator_set_by_timestamp(self, timestamp: int) -> Optional[List[Dict[str, Any]]]:
        """
        Get validator set by timestamp from Layer RPC
        """
        url = f"{self.layer_rpc_url}/layer/bridge/get_valset_by_timestamp/{timestamp}"
        data = self.make_request_with_retry(url)
        
        if data and 'bridge_validator_set' in data:
            try:
                validator_set = data['bridge_validator_set']
                logger.debug(f"Retrieved validator set for timestamp {timestamp}: {len(validator_set)} validators")
                return validator_set
            except (ValueError, TypeError) as e:
                logger.error(f"Failed to parse validator set for timestamp {timestamp}: {e}")
                return None
        
        logger.warning(f"No validator set found for timestamp {timestamp}")
        return None

    def log_failed_valset_fetch(self, timestamp: int, error: str):
        """
        Log failed validator set fetch to failure log file
        """
        try:
            failure_entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "validator_timestamp": timestamp,
                "error": str(error)
            }
            
            with open(self.valset_failure_log_file, 'a') as f:
                json.dump(failure_entry, f)
                f.write('\n')
                f.flush()
            
            logger.warning(f"Logged failed validator set fetch {timestamp} to {self.valset_failure_log_file}")
            
        except Exception as e:
            logger.error(f"Failed to write to validator set failure log: {e}")

    def write_validator_set_data(self, timestamp: int, checkpoint: str, validators: List[Dict[str, Any]]):
        """
        Write validator set data to database
        """
        try:
            scrape_timestamp = datetime.utcnow()
            
            for validator in validators:
                # prepare validator data for database insertion
                data = {
                    'scrape_timestamp': scrape_timestamp,
                    'valset_timestamp': timestamp,
                    'valset_checkpoint': checkpoint,
                    'ethereum_address': validator.get('ethereumAddress', validator.get('ethereum_address', '')),
                    'power': validator.get('power', '')
                }
                
                self.db.insert_layer_validator_set(data)
            
            logger.info(f"Saved validator set data for timestamp {timestamp} ({len(validators)} validators)")
            
        except Exception as e:
            logger.error(f"Failed to write validator set data to database: {e}")

    def fetch_validator_set_with_retry(self, timestamp: int, checkpoint: str) -> bool:
        """
        Fetch validator set with retry logic
        Returns True if successful, False if failed
        """
        logger.debug(f"Fetching validator set for timestamp {timestamp}")
        
        try:
            validator_set = self.get_validator_set_by_timestamp(timestamp)
            
            if validator_set:
                self.write_validator_set_data(timestamp, checkpoint, validator_set)
                return True
            else:
                error_msg = f"No validator set data returned from API"
                logger.warning(f"Failed to fetch validator set for timestamp {timestamp}: {error_msg}")
                self.log_failed_valset_fetch(timestamp, error_msg)
                return False
                
        except Exception as e:
            error_msg = f"Exception while fetching validator set: {e}"
            logger.error(f"Failed to fetch validator set for timestamp {timestamp}: {error_msg}")
            self.log_failed_valset_fetch(timestamp, error_msg)
            return False
    
    def fetch_checkpoint_with_exponential_backoff(self, timestamp: int, max_backoff: int = 600) -> Optional[Dict[str, Any]]:
        """
        Fetch a single checkpoint with exponential backoff on failure
        """
        logger.debug(f"Fetching checkpoint for timestamp {timestamp}")
        
        # first try the normal retry logic
        checkpoint_data = self.get_validator_checkpoint_params(timestamp)
        
        if checkpoint_data:
            return checkpoint_data
        
        logger.warning(f"Initial checkpoint fetch failed for timestamp {timestamp}")
        logger.info("Starting exponential backoff attempts...")
        
        # if initial attempt fails, try exponential backoff
        backoff_delay = INITIAL_DELAY
        
        for attempt in range(MAX_RETRIES):
            logger.info(f"Exponential backoff attempt {attempt + 1}/{MAX_RETRIES}: waiting {min(backoff_delay, max_backoff)} seconds...")
            time.sleep(min(backoff_delay, max_backoff))
            
            checkpoint_data = self.get_validator_checkpoint_params(timestamp)
            
            if checkpoint_data:
                logger.info(f"Exponential backoff succeeded! Retrieved checkpoint for timestamp {timestamp}")
                return checkpoint_data
            
            logger.warning(f"Exponential backoff attempt {attempt + 1} failed for timestamp {timestamp}")
            
            # if this is the last attempt, log failure
            if attempt == MAX_RETRIES - 1:
                error_msg = f"All {MAX_RETRIES} exponential backoff attempts failed"
                logger.error(f"All exponential backoff attempts failed for timestamp {timestamp}")
                self.log_failed_checkpoint(timestamp, error_msg)
                return None
            
            # double the delay for next attempt
            backoff_delay *= BACKOFF_MULTIPLIER
        
        return None
    
    def discover_checkpoints_by_index(self, state: Dict[str, Any]) -> List[tuple]:
        """
        Discover checkpoints by iterating through validator indices
        Returns list of (index, timestamp) tuples
        """
        logger.info("Discovering checkpoints by index...")
        
        last_processed_index = state.get('last_index', -1)
        last_processed_timestamp = state.get('last_timestamp', 0)
        current_timestamp = self.get_current_validator_set_timestamp()
        
        if not current_timestamp:
            logger.error("Could not get current validator set timestamp")
            return []
        
        # check if we're already caught up
        if last_processed_timestamp == current_timestamp:
            logger.debug(f"Already caught up (current timestamp: {current_timestamp}, last processed: {last_processed_timestamp})")
            return []
        
        checkpoints_to_process = []
        
        # start from the next index after last processed
        start_index = last_processed_index + 1
        logger.info(f"Starting scan from index {start_index} (looking for timestamps newer than {last_processed_timestamp})")
        
        index = start_index
        consecutive_failures = 0
        max_consecutive_failures = 3  # reduce to 3 since we know when to stop
        
        while consecutive_failures < max_consecutive_failures:
            timestamp = self.get_validator_timestamp_by_index(index)
            
            if timestamp is None:
                consecutive_failures += 1
                logger.debug(f"No timestamp found for index {index} (failure {consecutive_failures}/{max_consecutive_failures})")
                
                # if we're doing initial scan and hit failures early, they might be real gaps
                if start_index == 0 and index < 10:
                    logger.debug(f"Continuing through early indices despite failure at {index}")
                    index += 1
                    continue
                
                # if we've processed some indices and hit failures, we might be at the end
                if len(checkpoints_to_process) > 0:
                    logger.info(f"Stopping scan after {consecutive_failures} consecutive failures")
                    break
                    
                index += 1
                continue
            
            # reset failure counter on success
            consecutive_failures = 0
            
            # add this checkpoint to process
            checkpoints_to_process.append((index, timestamp))
            logger.debug(f"Found checkpoint at index {index}: timestamp {timestamp}")
            
            # check if we've reached the current timestamp
            if timestamp == current_timestamp:
                logger.info(f"Reached current timestamp {current_timestamp} at index {index}")
                break
            
            index += 1
            
            # small delay to avoid rate limiting
            time.sleep(0.1)
        
        logger.info(f"Found {len(checkpoints_to_process)} checkpoints to process (indices {start_index} to {index})")
        return checkpoints_to_process
    
    def lookup_validator_set_by_checkpoint(self, checkpoint: str) -> Optional[List[Dict[str, Any]]]:
        """
        Look up validator set data by checkpoint from the saved CSV
        Returns list of validator data or None if not found
        """
        try:
            # This function is no longer used as validator sets are stored in the database
            # Keeping it for now as it might be called elsewhere or for debugging
            # The actual lookup logic would need to be adapted to query the database
            logger.warning("lookup_validator_set_by_checkpoint is deprecated as validator sets are stored in the database.")
            return None
                
        except Exception as e:
            logger.error(f"Error looking up validator set by checkpoint {checkpoint}: {e}")
            return None

    def run_monitoring_cycle(self) -> Dict[str, Any]:
        """
        Run one monitoring cycle
        """
        # load current state
        state = self.load_state()
        
        # discover checkpoints by index
        checkpoints_to_process = self.discover_checkpoints_by_index(state)
        
        if not checkpoints_to_process:
            logger.debug("No new checkpoints to process")
            return state
        
        # process each checkpoint
        checkpoints_processed = 0
        validator_sets_processed = 0
        
        for index, timestamp in checkpoints_to_process:
            logger.info(f"Processing checkpoint at index {index}, timestamp {timestamp}")
            
            try:
                checkpoint_data = self.fetch_checkpoint_with_exponential_backoff(timestamp)
                
                if checkpoint_data:
                    # note: write_checkpoint_data now also fetches and saves validator set
                    success = self.write_checkpoint_data(index, checkpoint_data)
                    if success:
                        checkpoints_processed += 1
                        
                        # check if validator set was also successfully saved
                        # (this is a simple check - we could make it more sophisticated)
                        validator_sets_processed += 1
                        
                        # update state
                        state["last_index"] = index
                        state["last_timestamp"] = timestamp
                        state["total_checkpoints"] = state.get("total_checkpoints", 0) + 1
                        
                        # save state after each successful checkpoint
                        self.save_state(state)
                    
                    # add delay between checkpoints to avoid rate limiting
                    if len(checkpoints_to_process) > 1:
                        time.sleep(2)  # 2 second delay between checkpoints
                else:
                    logger.warning(f"Failed to fetch checkpoint for index {index}, timestamp {timestamp}")
                    
            except Exception as e:
                logger.error(f"Error processing checkpoint for index {index}, timestamp {timestamp}: {e}")
                self.log_failed_checkpoint(timestamp, str(e))
                continue
        
        logger.info(f"Monitoring cycle complete. Processed {checkpoints_processed} checkpoints and {validator_sets_processed} validator sets")
        logger.info(f"Total checkpoints found so far: {state['total_checkpoints']}")
        
        return state
    
    def run_continuous(self, interval_seconds: int = 300):
        """
        Run continuous monitoring with specified interval
        """
        logger.info(f"Starting continuous checkpoint and validator set monitoring (interval: {interval_seconds}s)")
        logger.info(f"Layer RPC: {self.layer_rpc_url}")
        logger.info(f"Chain ID: {self.chain_id}")
        logger.info(f"Database: {self.db.database_path}")
        logger.info(f"Failure logs: {self.failure_log_file}, {self.valset_failure_log_file}")
        
        try:
            while True:
                try:
                    self.run_monitoring_cycle()
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