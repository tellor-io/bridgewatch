#!/usr/bin/env python3
"""
Valset Alerter

This component monitors the evm_valset_updates table and sends Discord alerts
for every validator set update in the bridge contract.

Features:
- Monitors evm_valset_updates table for new entries
- Sends formatted Discord alerts with valset details
- Converts timestamps to US Eastern timezone  
- Looks up Ethereum block timestamps
- Maintains state for resuming from last processed update
- Handles database lock conflicts gracefully
"""

import time
import requests
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from web3 import Web3
import pytz
from config_manager import get_config_manager
import random
from ping_helper import PingHelper

# configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatabaseLockError(Exception):
    """Raised when database is locked by another process"""
    pass

class ValsetAlerter:
    def __init__(self, disable_discord: bool = False):
        """Initialize the valset alerter"""
        try:
            self.config_manager = get_config_manager()
            self.db = self.config_manager.create_database_manager()
            
            # get configuration
            self.bridge_contract = self.config_manager.get_bridge_contract()
            self.evm_rpc_url = self.config_manager.get_evm_rpc_url()
            self.layer_chain = self.config_manager.get_layer_chain()
            self.evm_chain = self.config_manager.get_evm_chain()
            
            # store disable_discord flag
            self.disable_discord = disable_discord
            
            # get Discord webhook for valset updates (unless disabled)
            self.discord_webhook_url = None if disable_discord else self.get_valset_discord_webhook()
            
            # initialize Web3 for block timestamp lookups
            self.w3 = Web3(Web3.HTTPProvider(self.evm_rpc_url))
            if not self.w3.is_connected():
                raise ConnectionError("Failed to connect to EVM RPC")
            
            # set up timezone converter
            self.eastern_tz = pytz.timezone('US/Eastern')
            
            # ping configuration
            self.ping_frequency_days = 7
            self.ping_helper = PingHelper(
                script_name="valset_alerter",
                data_dir=self.config_manager.get_data_dir(),
                discord_webhook_url=self.discord_webhook_url
            )
            
            logger.info(f"Initialized ValsetAlerter for {self.layer_chain} → {self.evm_chain}")
            logger.info(f"Bridge contract: {self.bridge_contract}")
            
            if self.disable_discord:
                logger.warning("Discord alerts: DISABLED via --no-discord flag")
            else:
                logger.info(f"Discord alerts: {'enabled' if self.discord_webhook_url else 'disabled'}")
            
        except Exception as e:
            logger.error(f"Failed to initialize ValsetAlerter: {e}")
            raise
    
    def is_database_lock_error(self, error: Exception) -> bool:
        """Check if error is a DuckDB lock conflict"""
        error_str = str(error).lower()
        return (
            "could not set lock on file" in error_str or
            "conflicting lock is held" in error_str or
            "database is locked" in error_str or
            "io error" in error_str and "lock" in error_str
        )
    
    def get_valset_discord_webhook(self) -> Optional[str]:
        """Get the Discord webhook URL for valset updates"""
        # try new discord_webhooks structure first
        webhooks = self.config_manager.get_active_config().get('discord_webhooks', {})
        if 'valset_updates' in webhooks:
            return webhooks['valset_updates']
        
        # fallback to legacy webhook (with warning)
        legacy_webhook = self.config_manager.get_discord_webhook_url()
        if legacy_webhook:
            logger.warning("Using legacy discord_webhook_url. Consider updating config to use discord_webhooks.valset_updates")
            return legacy_webhook
        
        # try environment variable
        import os
        env_webhook = os.getenv('DISCORD_WEBHOOK_VALSET_URL')
        if env_webhook:
            return env_webhook
            
        logger.warning("No Discord webhook configured for valset updates")
        return None
    
    def load_alerter_state(self) -> Dict[str, Any]:
        """Load alerter state from database"""
        try:
            state = self.db.get_component_state('valset_alerter')
            if state:
                logger.info(f"Resuming from database state: {state.get('total_alerts_sent', 0)} alerts sent, "
                           f"last update: {state.get('last_tx_hash', 'none')}")
                return state
            else:
                logger.info("No previous alerter state found in database, starting fresh")
                return {
                    "last_tx_hash": None,
                    "last_block_number": 0,
                    "total_alerts_sent": 0,
                    "last_alert_timestamp": None
                }
        except Exception as e:
            if self.is_database_lock_error(e):
                logger.warning(f"Database is locked by another process: {e}")
                raise DatabaseLockError(f"Database locked: {e}")
            else:
                logger.warning(f"Could not load alerter state from database: {e}")
                return {
                    "last_tx_hash": None,
                    "last_block_number": 0,
                    "total_alerts_sent": 0,
                    "last_alert_timestamp": None
                }
    
    def save_alerter_state(self, state: Dict[str, Any]):
        """Save alerter state to database"""
        try:
            self.db.save_component_state('valset_alerter', state)
            logger.debug(f"Saved alerter state to database: {state}")
        except Exception as e:
            if self.is_database_lock_error(e):
                logger.warning(f"Could not save alerter state due to database lock: {e}")
                raise DatabaseLockError(f"Database locked while saving state: {e}")
            else:
                logger.error(f"Could not save alerter state to database: {e}")
                raise
    
    def get_new_valset_updates(self, state: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get new validator set updates since last alert"""
        try:
            # get all valset updates from database
            all_updates = self.db.get_all_evm_valset_updates()
            
            if not state.get("last_tx_hash"):
                # first run, process all
                logger.info(f"First run: found {len(all_updates)} total valset updates")
                return all_updates
            
            # filter to only new updates
            last_tx_hash = state["last_tx_hash"]
            last_block_number = state["last_block_number"]
            
            # find the index of the last processed update
            last_index = -1
            for i, update in enumerate(all_updates):
                if update["tx_hash"] == last_tx_hash and update["block_number"] == last_block_number:
                    last_index = i
                    break
            
            if last_index >= 0:
                new_updates = all_updates[last_index + 1:]
                logger.info(f"Found {len(new_updates)} new valset updates since last run")
                return new_updates
            else:
                logger.warning(f"Could not find last processed update {last_tx_hash}, processing all")
                return all_updates
                
        except Exception as e:
            if self.is_database_lock_error(e):
                logger.warning(f"Database is locked by another process: {e}")
                raise DatabaseLockError(f"Database locked while getting valset updates: {e}")
            else:
                logger.error(f"Failed to get valset updates from database: {e}")
                return []
    
    def get_ethereum_block_timestamp(self, block_number: int) -> Optional[datetime]:
        """Get timestamp of an Ethereum block"""
        try:
            block = self.w3.eth.get_block(block_number)
            return datetime.fromtimestamp(block.timestamp, tz=pytz.UTC)
        except Exception as e:
            logger.error(f"Failed to get block timestamp for block {block_number}: {e}")
            return None
    
    def format_timestamp(self, timestamp_ms: int) -> str:
        """Convert millisecond timestamp to US Eastern time string"""
        try:
            # convert milliseconds to seconds
            timestamp_seconds = timestamp_ms / 1000
            # create UTC datetime
            utc_dt = datetime.fromtimestamp(timestamp_seconds, tz=pytz.UTC)
            # convert to Eastern time
            eastern_dt = utc_dt.astimezone(self.eastern_tz)
            return eastern_dt.strftime('%Y-%m-%d %H:%M:%S %Z')
        except Exception as e:
            logger.error(f"Failed to format timestamp {timestamp_ms}: {e}")
            return f"{timestamp_ms} ms (format error)"
    
    def send_valset_alert(self, update: Dict[str, Any]) -> bool:
        """Send Discord alert for a validator set update"""
        if self.disable_discord:
            logger.debug("Discord alerts disabled via --no-discord flag, skipping alert")
            return False
            
        if not self.discord_webhook_url:
            logger.debug("No Discord webhook configured, skipping alert")
            return False
        
        try:
            # get Ethereum block timestamp
            eth_block_time = self.get_ethereum_block_timestamp(update['block_number'])
            eth_time_str = "Unknown"
            if eth_block_time:
                eth_eastern = eth_block_time.astimezone(self.eastern_tz)
                eth_time_str = eth_eastern.strftime('%Y-%m-%d %H:%M:%S %Z')
            
            # format validator set timestamp
            valset_time_str = self.format_timestamp(update['validator_timestamp'])
            
            # create Discord embed
            embed = {
                "title": "🔄 Bridge Validator Set Updated",
                "description": f"Validator set updated in {self.layer_chain} → {self.evm_chain} bridge",
                "color": 0x3498db,  # blue
                "fields": [
                    {
                        "name": "💪 New Power Threshold",
                        "value": f"`{update['power_threshold']:,}`",
                        "inline": True
                    },
                    {
                        "name": "🕒 Validator Set Timestamp",
                        "value": f"`{update['validator_timestamp']:,}` ms\n{valset_time_str}",
                        "inline": True
                    },
                    {
                        "name": "📋 Checkpoint Hash",
                        "value": f"`{update['validator_set_hash'][:10]}...{update['validator_set_hash'][-8:]}`",
                        "inline": False
                    },
                    {
                        "name": "⛓️ Ethereum Details",
                        "value": f"**Block:** `{update['block_number']:,}`\n**Time:** {eth_time_str}",
                        "inline": True
                    },
                    {
                        "name": "🔗 Transaction",
                        "value": f"[View on Etherscan](https://{'sepolia.' if self.evm_chain == 'sepolia' else ''}etherscan.io/tx/{update['tx_hash']})",
                        "inline": True
                    }
                ],
                "footer": {
                    "text": f"Bridge Contract: {self.bridge_contract}"
                },
                "timestamp": datetime.utcnow().isoformat()
            }
            
            payload = {
                "username": "Bridge Monitor",
                "embeds": [embed]
            }
            
            response = requests.post(self.discord_webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            
            logger.info(f"✅ Sent Discord alert for valset update {update['tx_hash']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send Discord alert for {update['tx_hash']}: {e}")
            return False

    def generate_ping_content(self, state: Optional[Dict[str, Any]] = None) -> str:
        """Generate ping content summarizing current valset alerting status"""
        try:
            if state is None:
                state = self.load_alerter_state()
            last_tx = state.get('last_tx_hash', 'none')
            last_block = state.get('last_block_number', 0)
            total_alerts = state.get('total_alerts_sent', 0)
            last_alert_ts = state.get('last_alert_timestamp', 'Never')
            return (
                f"**Bridge:** {self.layer_chain} → {self.evm_chain}\n"
                f"**Total Alerts Sent:** {total_alerts}\n"
                f"**Last Alert Tx:** `{last_tx}`\n"
                f"**Last Block:** `{last_block}`\n"
                f"**Last Alert Time:** {last_alert_ts}"
            )
        except Exception as e:
            logger.error(f"Failed to generate ping content: {e}")
            return f"**Status:** Error generating ping content: {e}"
    
    def run_once(self) -> int:
        """Run once and process all new valset updates"""
        logger.info("Starting valset alerter (run once)")
        
        try:
            # load state - will raise DatabaseLockError if database is locked
            state = self.load_alerter_state()
            
            # get new updates - will raise DatabaseLockError if database is locked  
            new_updates = self.get_new_valset_updates(state)
            
        except DatabaseLockError as e:
            logger.warning(f"Skipping cycle due to database lock: {e}")
            return -1  # special return code indicating database lock
        
        if not new_updates:
            logger.info("No new valset updates to alert on")
            return 0
        
        logger.info(f"Processing {len(new_updates)} new valset updates")
        
        alerts_sent = 0
        for i, update in enumerate(new_updates):
            logger.info(f"Processing update {i+1}/{len(new_updates)}: {update['tx_hash']}")
            
            # send alert
            if self.send_valset_alert(update):
                alerts_sent += 1
            
            # update state after each alert
            state['last_tx_hash'] = update['tx_hash']
            state['last_block_number'] = update['block_number']
            state['total_alerts_sent'] = state.get('total_alerts_sent', 0) + 1
            state['last_alert_timestamp'] = datetime.utcnow().isoformat()
            
            try:
                self.save_alerter_state(state)
            except DatabaseLockError as e:
                logger.warning(f"Could not save state after alert due to database lock: {e}")
                # continue processing but state won't be saved until next successful save
            
            # small delay between alerts
            time.sleep(1)
        
        logger.info(f"✅ Sent {alerts_sent} valset update alerts")
        logger.info(f"📊 Total alerts sent: {state['total_alerts_sent']}")
        
        # scheduled weekly ping
        try:
            if self.ping_helper.should_send_ping(self.ping_frequency_days):
                ping_content = self.generate_ping_content(state)
                self.ping_helper.send_ping(ping_content, self.ping_frequency_days)
        except Exception as e:
            logger.warning(f"Ping check/send failed: {e}")
        
        return alerts_sent
    
    def run_continuous(self, interval_seconds: int = 60):
        """Run continuously, checking for new updates every interval"""
        logger.info(f"Starting valset alerter (continuous mode, interval: {interval_seconds}s)")
        
        consecutive_lock_errors = 0
        base_backoff_sleep = 5
        
        while True:
            try:
                alerts_sent = self.run_once()
                
                if alerts_sent == -1:  # database lock error
                    consecutive_lock_errors += 1
                    # exponential backoff for lock errors, but cap at 5 minutes
                    sleep_time = min(base_backoff_sleep * (2 ** (consecutive_lock_errors - 1)), 300)
                    # add a random jitter to the sleep time so we don't all retry at the same time
                    sleep_time = sleep_time * (1 + random.uniform(-0.1, 0.1))
                    logger.info(f"Database locked ({consecutive_lock_errors} consecutive), retrying in {sleep_time}s...")
                    time.sleep(sleep_time)
                    continue
                else:
                    # reset lock error counter on successful run
                    consecutive_lock_errors = 0
                    
                    if alerts_sent > 0:
                        logger.info(f"Cycle complete: sent {alerts_sent} alerts")
                    else:
                        logger.debug("Cycle complete: no new updates")
                
                logger.info(f"Sleeping for {interval_seconds} seconds...")
                time.sleep(interval_seconds)
                
            except KeyboardInterrupt:
                logger.info("Received interrupt signal, shutting down...")
                break
            except Exception as e:
                logger.error(f"Error in continuous loop: {e}")
                consecutive_lock_errors = 0  # reset since this isn't a lock error
                logger.info(f"Retrying in {interval_seconds} seconds...")
                time.sleep(interval_seconds)


def main():
    """Main entry point for valset alerter"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Valset Alerter - Send Discord alerts for bridge validator set updates"
    )
    
    parser.add_argument('--once', action='store_true', help='Run once instead of continuously')
    parser.add_argument('--interval', type=int, default=60, 
                       help='Check interval in seconds for continuous mode (default: 60)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    parser.add_argument('--no-discord', action='store_true', help='Disable Discord alerts')
    parser.add_argument('--config', type=str, help='Specify which configuration profile to use (overrides ACTIVE_CONFIG)')
    
    args = parser.parse_args()
    
    # set config override if --config flag was provided
    if args.config:
        from config import set_global_config_override
        set_global_config_override(args.config)
        print(f"Using configuration: {args.config}")
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # log warning if Discord is disabled
    if args.no_discord:
        logger.warning("⚠️  Discord alerts are DISABLED via --no-discord flag")
    
    try:
        alerter = ValsetAlerter(disable_discord=args.no_discord)
        
        if args.once:
            alerts_sent = alerter.run_once()
            if alerts_sent == -1:
                print("Skipped due to database lock")
                exit(2)
            else:
                print(f"Sent {alerts_sent} alerts")
        else:
            alerter.run_continuous(args.interval)
            
    except Exception as e:
        logger.error(f"Valset alerter failed: {e}")
        exit(1)


if __name__ == "__main__":
    main()
