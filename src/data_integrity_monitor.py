#!/usr/bin/env python3
"""
Data Integrity Monitor

This component monitors data integrity between TellorDataBank contracts and Tellor Layer,
verifying that relayed data matches the original data on the Layer blockchain.

Features:
- Monitors same queryIds from query_ids.json
- Tracks last checked index for each queryId 
- Checks new data entries on each interval
- Queries Tellor Layer API to verify data integrity
- Sends Discord alerts for data mismatches
- Maintains state between runs
"""

import time
import requests
import logging
import json
import argparse
from datetime import datetime
from typing import Optional, List, Dict, Any
from web3 import Web3
import pytz
from pathlib import Path
from config_manager import get_config_manager
from logger_utils import setup_logging

# logging will be configured in main() based on --verbose flag
logger = logging.getLogger(__name__)

class DataIntegrityMonitor:
    def __init__(self, disable_discord: bool = False, databank_contract_address: Optional[str] = None):
        """Initialize the data integrity monitor
        
        Args:
            disable_discord: If True, skip sending Discord alerts
            databank_contract_address: Override TellorDataBank contract address
        """
        try:
            self.config_manager = get_config_manager()
            
            # get configuration
            self.bridge_contract_address = self.config_manager.get_bridge_contract()
            self.evm_rpc_url = self.config_manager.get_evm_rpc_url()
            self.layer_chain = self.config_manager.get_layer_chain()
            self.layer_rpc_url = self.config_manager.get_layer_rpc_url()
            self.evm_chain = self.config_manager.get_evm_chain()
            
            # use provided databank address or derive from config
            self.databank_contract_address = databank_contract_address or self._get_databank_address()
            
            # store settings
            self.disable_discord = disable_discord
            
            # initialize Web3
            self.w3 = Web3(Web3.HTTPProvider(self.evm_rpc_url))
            
            # test connection
            try:
                latest_block = self.w3.eth.block_number
                logger.debug(f"Connected to EVM RPC. Latest block: {latest_block}")
            except Exception as e:
                raise ConnectionError(f"Failed to connect to EVM RPC: {e}")
            
            # load databank contract
            self.databank_contract = self._load_databank_contract()
            
            # load query IDs configuration (same as feed stale monitor)
            self.feeds_config = self._load_feeds_config()
            
            # get Discord webhook URL
            self.discord_webhook_url = None if disable_discord else self._get_data_integrity_discord_webhook()
            
            # set up timezone
            self.utc_tz = pytz.timezone('UTC')
            self.eastern_tz = pytz.timezone('US/Eastern')
            
            # state management for tracking last checked indices
            # put state file in data_dir/validation/
            data_dir = self.config_manager.get_data_dir()
            validation_dir = Path(data_dir) / "validation"
            validation_dir.mkdir(parents=True, exist_ok=True)
            self.state_file = validation_dir / "data_integrity_state.json"
            self.last_checked_indices = self._load_state()
            
            logger.info(f"Initialized DataIntegrityMonitor for {self.layer_chain} → {self.evm_chain}")
            logger.info(f"DataBank contract: {self.databank_contract_address}")
            logger.info(f"Layer RPC: {self.layer_rpc_url}")
            logger.info(f"Monitoring {len(self.feeds_config['feeds'])} feeds for data integrity")
            logger.info(f"Monitoring interval: {self.feeds_config['monitoring_interval_minutes']} minutes")
            
            if self.disable_discord:
                logger.warning("Discord alerts: DISABLED via --no-discord flag")
            else:
                logger.info(f"Discord alerts: {'enabled' if self.discord_webhook_url else 'disabled (no webhook configured)'}")
            
        except Exception as e:
            logger.error(f"Failed to initialize DataIntegrityMonitor: {e}")
            raise
    
    def _get_databank_address(self) -> str:
        """Get TellorDataBank contract address from configuration"""
        databank_address = self.config_manager.get_databank_contract()
        if not databank_address:
            raise ValueError("TellorDataBank contract address not configured. Please set 'databank_contract' in config.json or use --databank-address")
        return databank_address
    
    def _load_databank_contract(self):
        """Load the TellorDataBank contract ABI and create Web3 contract instance"""
        try:
            abi_path = "abis/TellorDataBank.json"
            with open(abi_path, 'r') as f:
                contract_data = json.load(f)
            
            contract = self.w3.eth.contract(
                address=self.databank_contract_address,
                abi=contract_data['abi']
            )
            
            logger.debug(f"Loaded TellorDataBank contract from {abi_path}")
            return contract
            
        except Exception as e:
            logger.error(f"Failed to load TellorDataBank contract: {e}")
            raise
    
    def _load_feeds_config(self) -> Dict[str, Any]:
        """Load feeds configuration from query_ids.json"""
        try:
            config_path = Path("query_ids.json")
            if not config_path.exists():
                raise FileNotFoundError(f"Config file {config_path} not found")
            
            with open(config_path, 'r') as f:
                config = json.load(f)
            
            logger.debug(f"Loaded feeds config from {config_path}")
            return config
            
        except Exception as e:
            logger.error(f"Failed to load feeds config: {e}")
            raise
    
    def _get_data_integrity_discord_webhook(self) -> Optional[str]:
        """Get Discord webhook URL for data integrity alerts"""
        try:
            # try to get from discord_webhooks configuration first
            webhooks = self.config_manager.get_discord_webhooks()
            if webhooks and 'malicious_activity' in webhooks:
                return webhooks['malicious_activity']
            
            # fallback to general discord webhook
            return self.config_manager.get_discord_webhook_url()
            
        except Exception as e:
            logger.warning(f"Could not get Discord webhook URL: {e}")
            return None
    
    def _load_state(self) -> Dict[str, int]:
        """Load last checked indices from state file"""
        try:
            if self.state_file.exists():
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                logger.debug(f"Loaded state from {self.state_file}")
                return state
            else:
                logger.info("No existing state file, starting fresh")
                return {}
        except Exception as e:
            logger.warning(f"Failed to load state: {e}, starting fresh")
            return {}
    
    def _save_state(self):
        """Save last checked indices to state file"""
        try:
            with open(self.state_file, 'w') as f:
                json.dump(self.last_checked_indices, f, indent=2)
            logger.debug(f"Saved state to {self.state_file}")
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
    
    def _initialize_feed_state(self, query_id: str) -> int:
        """Initialize state for a new feed by checking past 48 hours of data"""
        try:
            current_count = self.databank_contract.functions.getAggregateValueCount(query_id).call()
            
            if current_count == 0:
                logger.info(f"No data found for feed {query_id}")
                self.last_checked_indices[query_id] = -1
                return 0
            
            # calculate 48 hours ago timestamp (in milliseconds)
            hours_48_ago_ms = int((time.time() - (48 * 60 * 60)) * 1000)
            
            # start from the latest and work backwards to find data from 48 hours ago
            initial_index = current_count - 1  # default to latest if we can't find 48h data
            
            # look backwards through indices to find first entry older than 48 hours
            for index in range(current_count - 1, -1, -1):
                try:
                    aggregate_data = self.databank_contract.functions.getAggregateByIndex(query_id, index).call()
                    aggregate_timestamp = aggregate_data[2]  # aggregateTimestamp
                    
                    if aggregate_timestamp == 0:
                        continue
                    
                    # if this entry is older than 48 hours, we start from the next one
                    if aggregate_timestamp < hours_48_ago_ms:
                        initial_index = index  # start from this older entry
                        break
                    
                    # if we reach the first entry and it's still within 48h, start from beginning
                    if index == 0:
                        initial_index = 0
                        
                except Exception as e:
                    logger.warning(f"Error checking index {index} for {query_id}: {e}")
                    continue
            
            self.last_checked_indices[query_id] = initial_index
            entries_to_check = current_count - initial_index - 1
            
            logger.info(f"Initialized feed {query_id}: current count {current_count}, starting from index {initial_index}")
            logger.info(f"Will check {entries_to_check} entries from the past 48 hours on first run")
            
            return current_count
            
        except Exception as e:
            logger.error(f"Failed to initialize state for {query_id}: {e}")
            return 0
    
    def _query_tellor_layer(self, query_id: str, timestamp: int) -> Optional[Dict[str, Any]]:
        """Query Tellor Layer API to get aggregate data for verification"""
        try:
            # convert to hex format if needed (remove 0x prefix for API)
            if query_id.startswith('0x'):
                query_id_clean = query_id[2:]
            else:
                query_id_clean = query_id
            
            url = f"{self.layer_rpc_url}/tellor-io/layer/oracle/retrieve_data/{query_id_clean}/{timestamp}"
            
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            logger.debug(f"Retrieved data from Layer for {query_id} at {timestamp}")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to query Tellor Layer for {query_id} at {timestamp}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error processing Layer response for {query_id} at {timestamp}: {e}")
            return None
    
    def _format_timestamp(self, timestamp_ms: int) -> str:
        """Format timestamp in milliseconds to human-readable string"""
        if timestamp_ms == 0:
            return "N/A"
        
        timestamp_s = timestamp_ms / 1000
        dt = datetime.fromtimestamp(timestamp_s, tz=self.utc_tz)
        utc_str = dt.strftime('%Y-%m-%d %H:%M:%S UTC')
        eastern_dt = dt.astimezone(self.eastern_tz)
        eastern_str = eastern_dt.strftime('%Y-%m-%d %H:%M:%S %Z')
        
        return f"{utc_str} ({eastern_str})"
    
    def _compare_values(self, databank_value: bytes, layer_value: str) -> bool:
        """Compare values from DataBank and Layer"""
        try:
            # convert databank bytes to hex string (without 0x prefix)
            databank_hex = databank_value.hex()
            
            # layer value should be a hex string, normalize it
            layer_hex = layer_value.lower()
            if layer_hex.startswith('0x'):
                layer_hex = layer_hex[2:]
            
            # compare the hex strings
            matches = databank_hex.lower() == layer_hex
            
            if not matches:
                logger.warning(f"Value mismatch: DataBank={databank_hex}, Layer={layer_hex}")
            
            return matches
            
        except Exception as e:
            logger.error(f"Error comparing values: {e}")
            return False
    
    def check_data_integrity(self, feed: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Check data integrity for a single feed
        
        Returns:
            List of integrity violations found
        """
        query_id = feed['queryId']
        feed_name = feed['name']
        violations = []
        
        try:
            # get current count
            current_count = self.databank_contract.functions.getAggregateValueCount(query_id).call()
            
            # initialize if this is the first time we're checking this feed
            if query_id not in self.last_checked_indices:
                self._initialize_feed_state(query_id)
                self._save_state()
            
            last_checked = self.last_checked_indices[query_id]
            
            if current_count <= last_checked:
                logger.debug(f"No new data for {feed_name} (count: {current_count}, last checked: {last_checked})")
                return violations
            
            logger.info(f"Checking {feed_name}: indices {last_checked + 1} to {current_count - 1}")
            
            # check each new entry
            for index in range(last_checked + 1, current_count):
                try:
                    # get data from DataBank
                    aggregate_data = self.databank_contract.functions.getAggregateByIndex(query_id, index).call()
                    # aggregate_data tuple: (value, power, aggregateTimestamp, attestationTimestamp, relayTimestamp)
                    databank_value = aggregate_data[0]  # bytes value
                    aggregate_timestamp = aggregate_data[2]  # uint256 timestamp in milliseconds
                    relay_timestamp = aggregate_data[4]  # uint256 relay timestamp
                    
                    if aggregate_timestamp == 0:
                        logger.warning(f"Empty data at index {index} for {feed_name}")
                        continue
                    
                    # query Layer for the same data
                    layer_data = self._query_tellor_layer(query_id, aggregate_timestamp)
                    
                    if not layer_data or 'aggregate' not in layer_data:
                        logger.error(f"No Layer data found for {feed_name} at timestamp {aggregate_timestamp}")
                        violations.append({
                            'type': 'layer_query_failed',
                            'feed': feed,
                            'index': index,
                            'aggregate_timestamp': aggregate_timestamp,
                            'relay_timestamp': relay_timestamp,
                            'databank_value': databank_value.hex(),
                            'error': 'Failed to retrieve data from Layer'
                        })
                        continue
                    
                    layer_aggregate = layer_data['aggregate']
                    layer_value = layer_aggregate.get('aggregate_value', '')
                    
                    # compare values
                    if not self._compare_values(databank_value, layer_value):
                        violations.append({
                            'type': 'value_mismatch',
                            'feed': feed,
                            'index': index,
                            'aggregate_timestamp': aggregate_timestamp,
                            'relay_timestamp': relay_timestamp,
                            'databank_value': databank_value.hex(),
                            'layer_value': layer_value,
                            'layer_data': layer_aggregate
                        })
                    else:
                        logger.debug(f"✓ Index {index} for {feed_name}: values match")
                    
                except Exception as e:
                    logger.error(f"Error checking index {index} for {feed_name}: {e}")
                    violations.append({
                        'type': 'check_error',
                        'feed': feed,
                        'index': index,
                        'error': str(e)
                    })
            
            # update last checked index
            if current_count > 0:
                self.last_checked_indices[query_id] = current_count - 1
                
        except Exception as e:
            logger.error(f"Error checking data integrity for {feed_name}: {e}")
            violations.append({
                'type': 'feed_error',
                'feed': feed,
                'error': str(e)
            })
        
        return violations
    
    def send_discord_alert(self, violation: Dict[str, Any]):
        """Send Discord alert for data integrity violation"""
        if self.disable_discord or not self.discord_webhook_url:
            return
        
        try:
            feed = violation['feed']
            feed_name = feed['name']
            violation_type = violation['type']
            
            if violation_type == 'value_mismatch':
                databank_value = violation['databank_value']
                layer_value = violation['layer_value']
                timestamp_str = self._format_timestamp(violation['aggregate_timestamp'])
                
                alert_message = (
                    f"🚨 **Data Integrity Violation** - {feed_name}\n\n"
                    f"**Type:** Value Mismatch\n"
                    f"**Index:** {violation['index']}\n"
                    f"**Timestamp:** {timestamp_str}\n"
                    f"**DataBank Value:** `{databank_value}`\n"
                    f"**Layer Value:** `{layer_value}`\n"
                    f"**Query ID:** `{feed['queryId']}`\n"
                    f"**Chain:** {self.layer_chain} → {self.evm_chain}"
                )
            
            elif violation_type == 'layer_query_failed':
                timestamp_str = self._format_timestamp(violation['aggregate_timestamp'])
                
                alert_message = (
                    f"🚨 **Data Integrity Check Failed** - {feed_name}\n\n"
                    f"**Type:** Layer Query Failed\n"
                    f"**Index:** {violation['index']}\n"
                    f"**Timestamp:** {timestamp_str}\n"
                    f"**Error:** {violation['error']}\n"
                    f"**Query ID:** `{feed['queryId']}`\n"
                    f"**Chain:** {self.layer_chain} → {self.evm_chain}"
                )
            
            else:
                alert_message = (
                    f"🚨 **Data Integrity Error** - {feed_name}\n\n"
                    f"**Type:** {violation_type}\n"
                    f"**Error:** {violation.get('error', 'Unknown error')}\n"
                    f"**Query ID:** `{feed['queryId']}`\n"
                    f"**Chain:** {self.layer_chain} → {self.evm_chain}"
                )
            
            payload = {
                "content": alert_message,
                "username": "Data Integrity Monitor"
            }
            
            response = requests.post(self.discord_webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            
            logger.info(f"Sent Discord alert for {violation_type} in {feed_name}")
            
        except Exception as e:
            logger.error(f"Failed to send Discord alert: {e}")
    
    def check_all_feeds(self) -> List[Dict[str, Any]]:
        """Check data integrity for all configured feeds
        
        Returns:
            List of all violations found
        """
        all_violations = []
        
        for feed in self.feeds_config['feeds']:
            violations = self.check_data_integrity(feed)
            all_violations.extend(violations)
        
        return all_violations
    
    def run_once(self) -> List[Dict[str, Any]]:
        """Run data integrity check once and return results"""
        logger.info("Running data integrity check...")
        
        violations = self.check_all_feeds()
        
        if violations:
            logger.warning(f"Found {len(violations)} data integrity violations")
            for violation in violations:
                feed_name = violation['feed']['name']
                violation_type = violation['type']
                logger.warning(f"- {feed_name}: {violation_type}")
                
                # send Discord alert
                self.send_discord_alert(violation)
        else:
            logger.info("No data integrity violations found")
        
        # save state after check
        self._save_state()
        
        return violations
    
    def run_continuous(self):
        """Run continuous monitoring with configured interval"""
        interval_minutes = self.feeds_config['monitoring_interval_minutes']
        interval_seconds = interval_minutes * 60
        
        logger.info(f"Starting continuous data integrity monitoring (interval: {interval_minutes}m)")
        
        try:
            while True:
                try:
                    self.run_once()
                except Exception as e:
                    logger.error(f"Error during monitoring cycle: {e}")
                
                logger.debug(f"Sleeping for {interval_minutes} minutes...")
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            logger.info("Monitoring stopped by user")
        except Exception as e:
            logger.error(f"Monitoring stopped due to error: {e}")
            raise

def main():
    """Main entry point for the data integrity monitor"""
    parser = argparse.ArgumentParser(description='Monitor TellorDataBank data integrity against Tellor Layer')
    parser.add_argument('--once', action='store_true', help='Run once instead of continuously')
    parser.add_argument('--no-discord', action='store_true', help='Disable Discord alerts')
    parser.add_argument('--databank-address', type=str, help='Override TellorDataBank contract address')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose debug logging')
    
    args = parser.parse_args()
    
    # setup logging based on verbose flag
    setup_logging(verbose=args.verbose)
    
    try:
        # initialize monitor
        monitor = DataIntegrityMonitor(
            disable_discord=args.no_discord,
            databank_contract_address=args.databank_address
        )
        
        if args.once:
            # run once
            violations = monitor.run_once()
            if violations:
                exit(1)  # exit with error code if violations found
        else:
            # run continuously
            monitor.run_continuous()
            
    except Exception as e:
        logger.error(f"Monitor failed: {e}")
        exit(1)

if __name__ == "__main__":
    main() 