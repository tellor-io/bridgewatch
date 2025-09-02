#!/usr/bin/env python3
"""
Feed Stale Monitor

This component monitors data staleness in TellorDataBank contracts and sends 
Discord alerts when feeds become too old.

Features:
- Monitors multiple queryIds configured in query_ids.json
- Checks getCurrentAggregateData() for each queryId
- Compares aggregateTimestamp against configurable staleness threshold
- Sends Discord alerts with human-readable timestamps
- Supports run-once and continuous monitoring modes
- Separate from existing bridgewatch bot logic
"""

import time
import requests
import logging
import json
import argparse
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from web3 import Web3
import pytz
from pathlib import Path
from config_manager import get_config_manager
from logger_utils import setup_logging
from ping_helper import PingHelper

# logging will be configured in main() based on --verbose flag
logger = logging.getLogger(__name__)

class FeedStaleMonitor:
    def __init__(self, disable_discord: bool = False, databank_contract_address: Optional[str] = None,
                 ping_frequency_days: int = 7):
        """Initialize the feed stale monitor
        
        Args:
            disable_discord: If True, skip sending Discord alerts
            databank_contract_address: Override TellorDataBank contract address
            ping_frequency_days: Ping frequency in days (7=weekly, 1=daily, etc.)
        """
        try:
            self.config_manager = get_config_manager()
            
            # get configuration
            self.bridge_contract_address = self.config_manager.get_bridge_contract()
            self.evm_rpc_url = self.config_manager.get_evm_rpc_url()
            self.layer_chain = self.config_manager.get_layer_chain()
            self.evm_chain = self.config_manager.get_evm_chain()
            
            # use provided databank address or derive from config
            # note: this might need to be configured differently per bridge
            self.databank_contract_address = databank_contract_address or self._get_databank_address()
            
            # store settings
            self.disable_discord = disable_discord
            self.ping_frequency_days = ping_frequency_days
            
            # initialize Web3
            self.w3 = Web3(Web3.HTTPProvider(self.evm_rpc_url))
            
            # test connection by trying to get latest block
            try:
                latest_block = self.w3.eth.block_number
                logger.debug(f"Connected to EVM RPC. Latest block: {latest_block}")
            except Exception as e:
                raise ConnectionError(f"Failed to connect to EVM RPC: {e}")
            
            # load databank contract ABI and create contract instance
            self.databank_contract = self._load_databank_contract()
            
            # load query IDs configuration
            self.feeds_config = self._load_feeds_config()
            
            # get Discord webhook URL (unless disabled)
            self.discord_webhook_url = None if disable_discord else self._get_feed_stale_discord_webhook()
            
            # set up timezone for human-readable timestamps
            self.utc_tz = pytz.timezone('UTC')
            self.eastern_tz = pytz.timezone('US/Eastern')
            
            # alert state management to prevent duplicate alerts
            # put alert state file in data_dir/validation/
            data_dir = self.config_manager.get_data_dir()
            validation_dir = Path(data_dir) / "validation"
            validation_dir.mkdir(parents=True, exist_ok=True)
            self.alert_state_file = validation_dir / "last_feed_staleness_alert.json"
            self.last_alerted_timestamps = self._load_alert_state()
            
            # initialize ping helper
            self.ping_helper = PingHelper(
                script_name="feed_stale_monitor",
                data_dir=data_dir,
                discord_webhook_url=self.discord_webhook_url
            )
            
            logger.info(f"Initialized FeedStaleMonitor for {self.layer_chain} → {self.evm_chain}")
            logger.info(f"DataBank contract: {self.databank_contract_address}")
            logger.info(f"Monitoring {len(self.feeds_config['feeds'])} feeds")
            logger.info(f"Staleness threshold: {self.feeds_config['staleness_threshold_hours']} hours")
            logger.info(f"Monitoring interval: {self.feeds_config['monitoring_interval_minutes']} minutes")
            
            if self.disable_discord:
                logger.warning("Discord alerts: DISABLED via --no-discord flag")
            else:
                logger.info(f"Discord alerts: {'enabled' if self.discord_webhook_url else 'disabled (no webhook configured)'}")
            
        except Exception as e:
            logger.error(f"Failed to initialize FeedStaleMonitor: {e}")
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
            # load ABI from file
            abi_path = "abis/TellorDataBank.json"
            with open(abi_path, 'r') as f:
                contract_data = json.load(f)
            
            # create contract instance
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
                logger.error(f"Feeds config file {config_path} not found")
                raise FileNotFoundError(f"Config file {config_path} not found")
            
            with open(config_path, 'r') as f:
                config = json.load(f)
            
            # validate required fields
            required_fields = ['feeds', 'staleness_threshold_hours', 'monitoring_interval_minutes']
            for field in required_fields:
                if field not in config:
                    raise ValueError(f"Missing required field '{field}' in feeds config")
            
            # validate feeds structure
            for i, feed in enumerate(config['feeds']):
                required_feed_fields = ['queryId', 'name']
                for field in required_feed_fields:
                    if field not in feed:
                        raise ValueError(f"Missing required field '{field}' in feed {i}")
            
            logger.debug(f"Loaded feeds config from {config_path}")
            return config
            
        except Exception as e:
            logger.error(f"Failed to load feeds config: {e}")
            raise
    
    def _get_feed_stale_discord_webhook(self) -> Optional[str]:
        """Get Discord webhook URL for feed stale alerts"""
        try:
            # try to get from discord_webhooks configuration first
            webhook_url = self.config_manager.get_discord_webhook('feed_stale')
            if webhook_url and webhook_url != "N/A":
                return webhook_url
            
            # fallback to general discord webhook
            return self.config_manager.get_discord_webhook_url()
            
        except Exception as e:
            logger.warning(f"Could not get Discord webhook URL: {e}")
            return None
    
    def _load_alert_state(self) -> Dict[str, int]:
        """Load last alerted timestamps from alert state file"""
        try:
            if self.alert_state_file.exists():
                with open(self.alert_state_file, 'r') as f:
                    state = json.load(f)
                logger.debug(f"Loaded alert state from {self.alert_state_file}")
                return state
            else:
                logger.debug("No existing alert state file, starting fresh")
                return {}
        except Exception as e:
            logger.warning(f"Failed to load alert state: {e}, starting fresh")
            return {}
    
    def _save_alert_state(self):
        """Save last alerted timestamps to alert state file"""
        try:
            with open(self.alert_state_file, 'w') as f:
                json.dump(self.last_alerted_timestamps, f, indent=2)
            logger.debug(f"Saved alert state to {self.alert_state_file}")
        except Exception as e:
            logger.error(f"Failed to save alert state: {e}")
    
    def _should_alert_for_staleness(self, query_id: str, aggregate_timestamp: int) -> bool:
        """Check if we should alert for this stale timestamp (haven't alerted for it before)"""
        last_alerted = self.last_alerted_timestamps.get(query_id, 0)
        return aggregate_timestamp > last_alerted
    
    def _record_staleness_alert(self, query_id: str, aggregate_timestamp: int):
        """Record that we've alerted for this timestamp"""
        self.last_alerted_timestamps[query_id] = aggregate_timestamp
        self._save_alert_state()
    
    def _format_timestamp(self, timestamp_ms: int) -> str:
        """Format timestamp in milliseconds to human-readable string"""
        if timestamp_ms == 0:
            return "N/A"
        
        # convert from milliseconds to seconds
        timestamp_s = timestamp_ms / 1000
        dt = datetime.fromtimestamp(timestamp_s, tz=self.utc_tz)
        
        # format in both UTC and Eastern time
        utc_str = dt.strftime('%Y-%m-%d %H:%M:%S UTC')
        eastern_dt = dt.astimezone(self.eastern_tz)
        eastern_str = eastern_dt.strftime('%Y-%m-%d %H:%M:%S %Z')
        
        return f"{utc_str} ({eastern_str})"
    
    def _calculate_age_hours(self, timestamp_ms: int) -> float:
        """Calculate age in hours from timestamp in milliseconds"""
        if timestamp_ms == 0:
            return float('inf')
        
        now_ms = int(time.time() * 1000)
        age_ms = now_ms - timestamp_ms
        return age_ms / (1000 * 60 * 60)  # convert to hours
    
    def check_feed_staleness(self, feed: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Check if a single feed is stale
        
        Args:
            feed: Feed configuration with queryId and name
            
        Returns:
            Dictionary with staleness info if stale, None if fresh
        """
        try:
            query_id = feed['queryId']
            feed_name = feed['name']
            
            # call getCurrentAggregateData
            try:
                aggregate_data = self.databank_contract.functions.getCurrentAggregateData(query_id).call()
            except Exception as e:
                logger.error(f"Failed to get aggregate data for {feed_name}: {e}")
                return None
            
            # extract timestamps (in milliseconds)
            # aggregate_data tuple: (value, power, aggregateTimestamp, attestationTimestamp, relayTimestamp)
            aggregate_timestamp = aggregate_data[2]  # aggregateTimestamp
            relay_timestamp = aggregate_data[4]      # relayTimestamp
            
            # check if data exists
            if aggregate_timestamp == 0:
                logger.warning(f"No data found for feed {feed_name}")
                return None
            
            # calculate age
            age_hours = self._calculate_age_hours(aggregate_timestamp)
            threshold_hours = self.feeds_config['staleness_threshold_hours']
            
            # check if stale
            if age_hours > threshold_hours:
                # check if we've already alerted for this exact timestamp
                if not self._should_alert_for_staleness(query_id, aggregate_timestamp):
                    logger.debug(f"Feed {feed_name} is stale but already alerted for timestamp {aggregate_timestamp}")
                    return None
                
                return {
                    'feed': feed,
                    'aggregate_timestamp': aggregate_timestamp,
                    'relay_timestamp': relay_timestamp,
                    'age_hours': age_hours,
                    'threshold_hours': threshold_hours,
                    'is_stale': True
                }
            else:
                logger.debug(f"Feed {feed_name} is fresh: {age_hours:.1f}h old")
                return None
                
        except Exception as e:
            logger.error(f"Error checking feed staleness for {feed.get('name', 'unknown')}: {e}")
            return None
    
    def get_feed_status_for_ping(self, feed: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Get feed status for ping (returns info regardless of staleness)
        
        Args:
            feed: Feed configuration with queryId and name
            
        Returns:
            Dictionary with feed info or None if no data/error
        """
        try:
            query_id = feed['queryId']
            feed_name = feed['name']
            
            # call getCurrentAggregateData
            try:
                aggregate_data = self.databank_contract.functions.getCurrentAggregateData(query_id).call()
            except Exception as e:
                logger.error(f"Failed to get aggregate data for {feed_name}: {e}")
                return None
            
            # extract timestamps (in milliseconds)
            # aggregate_data tuple: (value, power, aggregateTimestamp, attestationTimestamp, relayTimestamp)
            aggregate_timestamp = aggregate_data[2]  # aggregateTimestamp
            relay_timestamp = aggregate_data[4]      # relayTimestamp
            
            # check if data exists
            if aggregate_timestamp == 0:
                logger.debug(f"No data found for feed {feed_name}")
                return None
            
            # calculate age
            age_hours = self._calculate_age_hours(aggregate_timestamp)
            threshold_hours = self.feeds_config['staleness_threshold_hours']
            
            return {
                'feed': feed,
                'aggregate_timestamp': aggregate_timestamp,
                'relay_timestamp': relay_timestamp,
                'age_hours': age_hours,
                'threshold_hours': threshold_hours,
                'is_stale': age_hours > threshold_hours
            }
                
        except Exception as e:
            logger.error(f"Error getting feed status for {feed.get('name', 'unknown')}: {e}")
            return None

    def generate_ping_content(self) -> str:
        """Generate ping content with current monitoring status"""
        try:
            ping_content = (
                f"**Monitoring Feed Staleness:**\n"
                f"**DataBank Contract:** `{self.databank_contract_address}`\n"
                f"**Chain:** {self.evm_chain}\n"
                f"**Feeds Monitored:** {len(self.feeds_config['feeds'])}\n"
                f"**Staleness Threshold:** {self.feeds_config['staleness_threshold_hours']} hours\n\n"
                f"**Current Status for Each Feed:**\n"
            )
            
            for feed in self.feeds_config['feeds']:
                query_id = feed['queryId']
                feed_name = feed['name']
                
                try:
                    # get current status for this feed (regardless of staleness)
                    feed_status = self.get_feed_status_for_ping(feed)
                    
                    if feed_status:
                        aggregate_timestamp = feed_status['aggregate_timestamp']
                        age_hours = feed_status['age_hours']
                        is_stale = feed_status['is_stale']
                        
                        # format timestamp for display
                        formatted_time = self.ping_helper.format_timestamp_et(aggregate_timestamp)
                        
                        # show status with timestamp
                        status = "STALE" if is_stale else "Fresh"
                        ping_content += f"**{feed_name}:** {status} - {aggregate_timestamp} ({formatted_time}) - {age_hours:.1f}h old\n"
                    else:
                        ping_content += f"**{feed_name}:** No data available\n"
                        
                except Exception as e:
                    ping_content += f"**{feed_name}:** Error checking status - {str(e)}\n"
            
            return ping_content
            
        except Exception as e:
            logger.error(f"Failed to generate ping content: {e}")
            return f"**Status:** Error generating ping content: {e}"

    def send_discord_alert(self, stale_info: Dict[str, Any]):
        """Send Discord alert for stale feed"""
        if self.disable_discord or not self.discord_webhook_url:
            return
        
        try:
            feed = stale_info['feed']
            feed_name = feed['name']
            age_hours = stale_info['age_hours']
            aggregate_timestamp = stale_info['aggregate_timestamp']
            relay_timestamp = stale_info['relay_timestamp']
            
            # format timestamps
            aggregate_time_str = self._format_timestamp(aggregate_timestamp)
            relay_time_str = self._format_timestamp(relay_timestamp)
            
            # create alert message
            alert_message = (
                f"🚨 **{feed_name} Feed Stale** - Data is {age_hours:.1f} hours old\n\n"
                f"**Report timestamp:** {aggregate_time_str}\n"
                f"**Relay timestamp:** {relay_time_str}\n"
                f"**Query ID:** `{feed['queryId']}`\n"
                f"**Chain:** {self.layer_chain} → {self.evm_chain}"
            )
            
            # send to Discord
            payload = {
                "content": alert_message,
                "username": "Feed Stale Monitor"
            }
            
            response = requests.post(self.discord_webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            
            logger.info(f"Sent Discord alert for stale feed: {feed_name}")
            
            # record that we've alerted for this timestamp to prevent duplicates
            self._record_staleness_alert(feed['queryId'], aggregate_timestamp)
            
        except Exception as e:
            logger.error(f"Failed to send Discord alert: {e}")
    
    def check_all_feeds(self) -> List[Dict[str, Any]]:
        """Check all configured feeds for staleness
        
        Returns:
            List of stale feed info dictionaries
        """
        stale_feeds = []
        
        for feed in self.feeds_config['feeds']:
            stale_info = self.check_feed_staleness(feed)
            if stale_info:
                stale_feeds.append(stale_info)
        
        return stale_feeds
    
    def run_once(self, send_ping: bool = False) -> List[Dict[str, Any]]:
        """Run staleness check once and return results"""
        logger.info("Running feed staleness check...")
        
        stale_feeds = self.check_all_feeds()
        
        if stale_feeds:
            logger.warning(f"Found {len(stale_feeds)} stale feeds")
            for stale_info in stale_feeds:
                feed_name = stale_info['feed']['name']
                age_hours = stale_info['age_hours']
                logger.warning(f"- {feed_name}: {age_hours:.1f}h old")
                
                # send Discord alert
                self.send_discord_alert(stale_info)
        else:
            logger.info("All feeds are fresh")
        
        # check for scheduled ping
        if self.ping_helper.should_send_ping(self.ping_frequency_days) or send_ping:
            ping_content = self.generate_ping_content()
            logger.debug(f"Generated ping content: {ping_content}")
            self.ping_helper.send_ping(ping_content, self.ping_frequency_days, force=send_ping)
        
        return stale_feeds
    
    def run_continuous(self, send_initial_ping: bool = False):
        """Run continuous monitoring with configured interval"""
        interval_minutes = self.feeds_config['monitoring_interval_minutes']
        interval_seconds = interval_minutes * 60
        
        logger.info(f"Starting continuous feed staleness monitoring (interval: {interval_minutes}m)")
        
        try:
            # First run - check if we should send initial ping
            first_run = True
            while True:
                try:
                    send_ping_now = send_initial_ping and first_run
                    self.run_once(send_ping=send_ping_now)
                    first_run = False
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
    """Main entry point for the feed stale monitor"""
    parser = argparse.ArgumentParser(description='Monitor TellorDataBank feeds for staleness')
    parser.add_argument('--once', action='store_true', help='Run once instead of continuously')
    parser.add_argument('--no-discord', action='store_true', help='Disable Discord alerts')
    parser.add_argument('--databank-address', type=str, help='Override TellorDataBank contract address')
    parser.add_argument('--ping-frequency', type=int, default=7, help='Ping frequency in days (default: 7)')
    parser.add_argument('--ping-now', action='store_true', help='Send ping immediately')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose debug logging')
    parser.add_argument('--config', type=str, help='Specify which configuration profile to use (overrides ACTIVE_CONFIG)')
    
    args = parser.parse_args()
    
    # set config override if --config flag was provided
    if args.config:
        from config import set_global_config_override
        set_global_config_override(args.config)
    
    # setup logging based on verbose flag
    setup_logging(verbose=args.verbose)
    
    # log which config we're using if override was provided
    if args.config:
        logger.info(f"🔧 Using configuration: {args.config}")
    
    try:
        # initialize monitor
        monitor = FeedStaleMonitor(
            disable_discord=args.no_discord,
            databank_contract_address=args.databank_address,
            ping_frequency_days=args.ping_frequency
        )
        
        if args.once:
            # run once
            stale_feeds = monitor.run_once(send_ping=args.ping_now)
            if stale_feeds:
                exit(1)  # exit with error code if stale feeds found
        else:
            # run continuously
            monitor.run_continuous(send_initial_ping=args.ping_now)
            
    except Exception as e:
        logger.error(f"Monitor failed: {e}")
        exit(1)

if __name__ == "__main__":
    main() 