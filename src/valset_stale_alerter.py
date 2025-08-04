#!/usr/bin/env python3
"""
Valset Stale Alerter

This component monitors the validator timestamp in the bridge contract and sends 
Discord alerts when the validator set becomes too old (stale).

Features:
- Queries validatorTimestamp() from the bridge contract
- Checks if timestamp is older than configurable threshold
- Sends Discord alerts when validator set is stale
- Configurable stale threshold (default: 2 weeks)
- Supports run-once and continuous monitoring modes
"""

import time
import requests
import logging
from datetime import datetime, timedelta
from typing import Optional
from web3 import Web3
import json
import pytz
from config_manager import get_config_manager

# configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ValsetStaleAlerter:
    def __init__(self, disable_discord: bool = False, stale_threshold_hours: int = 336):  # 2 weeks default
        """Initialize the valset stale alerter
        
        Args:
            disable_discord: If True, skip sending Discord alerts
            stale_threshold_hours: Hours after which validator set is considered stale (default: 336 = 2 weeks)
        """
        try:
            self.config_manager = get_config_manager()
            
            # get configuration
            self.bridge_contract_address = self.config_manager.get_bridge_contract()
            self.evm_rpc_url = self.config_manager.get_evm_rpc_url()
            self.layer_chain = self.config_manager.get_layer_chain()
            self.evm_chain = self.config_manager.get_evm_chain()
            
            # store settings
            self.disable_discord = disable_discord
            self.stale_threshold_hours = stale_threshold_hours
            self.stale_threshold_ms = stale_threshold_hours * 60 * 60 * 1000  # convert to milliseconds
            
            # initialize Web3
            self.w3 = Web3(Web3.HTTPProvider(self.evm_rpc_url))
            
            # test connection by trying to get latest block
            try:
                latest_block = self.w3.eth.block_number
                logger.debug(f"Connected to EVM RPC. Latest block: {latest_block}")
            except Exception as e:
                raise ConnectionError(f"Failed to connect to EVM RPC: {e}")
            
            # load bridge contract ABI and create contract instance
            self.bridge_contract = self._load_bridge_contract()
            
            # get Discord webhook URL (unless disabled)
            self.discord_webhook_url = None if disable_discord else self._get_stale_valset_discord_webhook()
            
            # set up timezone for human-readable timestamps
            self.utc_tz = pytz.timezone('UTC')
            self.eastern_tz = pytz.timezone('US/Eastern')
            
            logger.info(f"Initialized ValsetStaleAlerter for {self.layer_chain} → {self.evm_chain}")
            logger.info(f"Bridge contract: {self.bridge_contract_address}")
            logger.info(f"Stale threshold: {stale_threshold_hours} hours ({self.stale_threshold_hours/24:.1f} days)")
            
            if self.disable_discord:
                logger.warning("Discord alerts: DISABLED via --no-discord flag")
            else:
                logger.info(f"Discord alerts: {'enabled' if self.discord_webhook_url else 'disabled (no webhook configured)'}")
            
        except Exception as e:
            logger.error(f"Failed to initialize ValsetStaleAlerter: {e}")
            raise
    
    def _load_bridge_contract(self):
        """Load the bridge contract ABI and create Web3 contract instance"""
        try:
            # load ABI from file
            abi_path = "abis/TellorDataBridge.json"
            with open(abi_path, 'r') as f:
                contract_data = json.load(f)
            
            # create contract instance
            contract = self.w3.eth.contract(
                address=self.bridge_contract_address,
                abi=contract_data['abi']
            )
            
            logger.debug(f"Loaded bridge contract from {abi_path}")
            return contract
            
        except Exception as e:
            logger.error(f"Failed to load bridge contract: {e}")
            raise
    
    def _get_stale_valset_discord_webhook(self) -> Optional[str]:
        """Get the Discord webhook URL for stale validator set alerts"""
        # try new discord_webhooks structure first 
        webhooks = self.config_manager.get_active_config().get('discord_webhooks', {})
        if 'valset_stale' in webhooks:
            return webhooks['valset_stale']
        
        # fallback to valset_updates webhook
        if 'valset_updates' in webhooks:
            logger.info("Using valset_updates webhook for stale alerts (consider configuring discord_webhooks.valset_stale)")
            return webhooks['valset_updates']
        
        # fallback to legacy webhook (with warning)
        legacy_webhook = self.config_manager.get_discord_webhook_url()
        if legacy_webhook:
            logger.warning("Using legacy discord_webhook_url for stale alerts. Consider updating config to use discord_webhooks.valset_stale")
            return legacy_webhook
        
        # try environment variable
        import os
        env_webhook = os.getenv('DISCORD_WEBHOOK_VALSET_STALE_URL')
        if env_webhook:
            return env_webhook
            
        logger.warning("No Discord webhook configured for stale valset alerts")
        return None
    
    def get_validator_timestamp(self) -> Optional[int]:
        """Get the current validator timestamp from the bridge contract"""
        try:
            # call validatorTimestamp() function
            timestamp = self.bridge_contract.functions.validatorTimestamp().call()
            logger.debug(f"Current validator timestamp: {timestamp}")
            return timestamp
            
        except Exception as e:
            logger.error(f"Failed to get validator timestamp from bridge contract: {e}")
            return None
    
    def is_validator_set_stale(self, validator_timestamp: int) -> tuple[bool, timedelta]:
        """Check if validator set is stale
        
        Returns:
            tuple: (is_stale: bool, age: timedelta)
        """
        try:
            # current time in milliseconds
            current_time_ms = int(datetime.utcnow().timestamp() * 1000)
            
            # calculate age
            age_ms = current_time_ms - validator_timestamp
            age = timedelta(milliseconds=age_ms)
            
            # check if stale
            is_stale = age_ms > self.stale_threshold_ms
            
            logger.debug(f"Validator set age: {age}, threshold: {timedelta(hours=self.stale_threshold_hours)}, stale: {is_stale}")
            return is_stale, age
            
        except Exception as e:
            logger.error(f"Failed to check if validator set is stale: {e}")
            return False, timedelta()
    
    def format_timestamp(self, timestamp_ms: int) -> str:
        """Format timestamp for display"""
        try:
            dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=self.utc_tz)
            eastern = dt.astimezone(self.eastern_tz)
            return eastern.strftime('%Y-%m-%d %H:%M:%S %Z')
        except Exception:
            return f"Invalid timestamp: {timestamp_ms}"
    
    def send_stale_alert(self, validator_timestamp: int, age: timedelta) -> bool:
        """Send Discord alert for stale validator set"""
        if self.disable_discord:
            logger.debug("Discord alerts disabled via --no-discord flag, skipping alert")
            return False
            
        if not self.discord_webhook_url:
            logger.debug("No Discord webhook configured, skipping alert")
            return False
        
        try:
            # format timestamp
            timestamp_str = self.format_timestamp(validator_timestamp)
            
            # calculate days and hours
            total_hours = int(age.total_seconds() // 3600)
            days = total_hours // 24
            hours = total_hours % 24
            
            age_str = f"{days} days, {hours} hours" if days > 0 else f"{hours} hours"
            
            # create Discord embed
            embed = {
                "title": "⚠️ STALE VALIDATOR SET DETECTED",
                "description": f"Validator set in {self.layer_chain} → {self.evm_chain} bridge is stale and needs updating",
                "color": 0xFFA500,  # orange
                "fields": [
                    {
                        "name": "🕒 Validator Set Timestamp",
                        "value": f"`{validator_timestamp:,}` ms\n{timestamp_str}",
                        "inline": True
                    },
                    {
                        "name": "⏰ Age",
                        "value": f"**{age_str}**",
                        "inline": True
                    },
                    {
                        "name": "📏 Threshold",
                        "value": f"{self.stale_threshold_hours} hours ({self.stale_threshold_hours/24:.1f} days)",
                        "inline": True
                    },
                    {
                        "name": "🔗 Bridge Contract",
                        "value": f"`{self.bridge_contract_address}`",
                        "inline": False
                    },
                    {
                        "name": "⚠️ Action Required",
                        "value": "The validator set should be updated to maintain bridge security",
                        "inline": False
                    }
                ],
                "footer": {
                    "text": f"Bridge: {self.layer_chain} → {self.evm_chain}"
                },
                "timestamp": datetime.utcnow().isoformat()
            }
            
            payload = {
                "username": "Bridge Monitor",
                "embeds": [embed]
            }
            
            response = requests.post(self.discord_webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            
            logger.info(f"✅ Sent Discord alert for stale validator set (age: {age_str})")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send Discord alert for stale validator set: {e}")
            return False
    
    def check_once(self) -> bool:
        """Check validator set staleness once
        
        Returns:
            bool: True if validator set is stale, False if fresh or error
        """
        logger.info("Checking validator set staleness...")
        
        # get current validator timestamp
        validator_timestamp = self.get_validator_timestamp()
        if validator_timestamp is None:
            logger.error("Could not get validator timestamp from bridge contract")
            return False
        
        # check if stale
        is_stale, age = self.is_validator_set_stale(validator_timestamp)
        
        if is_stale:
            logger.warning(f"🚨 Validator set is STALE: {age}")
            # send alert
            alert_sent = self.send_stale_alert(validator_timestamp, age)
            if alert_sent:
                logger.info("Discord alert sent for stale validator set")
            return True
        else:
            logger.info(f"✅ Validator set is fresh (age: {age})")
            return False
    
    def run_once(self) -> int:
        """Run once and check validator set staleness
        
        Returns:
            int: 1 if stale, 0 if fresh, -1 if error
        """
        logger.info("Starting valset stale alerter (run once)")
        
        try:
            is_stale = self.check_once()
            return 1 if is_stale else 0
        except Exception as e:
            logger.error(f"Error checking validator set staleness: {e}")
            return -1
    
    def run_continuous(self, interval_seconds: int = 300):  # 5 minutes default
        """Run continuously and check validator set staleness at intervals"""
        logger.info(f"Starting valset stale alerter (continuous mode, interval: {interval_seconds}s)")
        
        last_alert_time = None
        alert_cooldown_hours = 6  # only alert every 6 hours for the same stale condition
        
        while True:
            try:
                # get current validator timestamp
                validator_timestamp = self.get_validator_timestamp()
                if validator_timestamp is None:
                    logger.error("Could not get validator timestamp from bridge contract")
                    time.sleep(interval_seconds)
                    continue
                
                # check if stale
                is_stale, age = self.is_validator_set_stale(validator_timestamp)
                
                if is_stale:
                    # check cooldown to avoid spam
                    now = datetime.utcnow()
                    should_alert = (last_alert_time is None or 
                                  (now - last_alert_time).total_seconds() > alert_cooldown_hours * 3600)
                    
                    if should_alert:
                        logger.warning(f"🚨 Validator set is STALE: {age}")
                        alert_sent = self.send_stale_alert(validator_timestamp, age)
                        if alert_sent:
                            last_alert_time = now
                            logger.info(f"Discord alert sent (next alert in {alert_cooldown_hours}h)")
                    else:
                        time_until_next = alert_cooldown_hours * 3600 - (now - last_alert_time).total_seconds()
                        logger.info(f"Validator set still stale (age: {age}), next alert in {time_until_next/3600:.1f}h")
                else:
                    logger.debug(f"✅ Validator set is fresh (age: {age})")
                    # reset alert time if validator set becomes fresh
                    if last_alert_time is not None:
                        logger.info("Validator set is fresh again, resetting alert cooldown")
                        last_alert_time = None
                
            except Exception as e:
                logger.error(f"Error in continuous monitoring: {e}")
            
            time.sleep(interval_seconds)


def main():
    """Main entry point for valset stale alerter"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Valset Stale Alerter - Send Discord alerts when bridge validator set is too old"
    )
    
    parser.add_argument('--once', action='store_true', help='Run once instead of continuously')
    parser.add_argument('--interval', type=int, default=300, 
                       help='Check interval in seconds for continuous mode (default: 300)')
    parser.add_argument('--stale-threshold', type=int, default=336,
                       help='Hours after which validator set is considered stale (default: 336 = 2 weeks)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    parser.add_argument('--no-color', action='store_true', help='Disable colored output')
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
        alerter = ValsetStaleAlerter(
            disable_discord=args.no_discord,
            stale_threshold_hours=args.stale_threshold
        )
        
        if args.once:
            result = alerter.run_once()
            if result == 1:
                print("Validator set is STALE")
                exit(1)
            elif result == 0:
                print("Validator set is fresh")
                exit(0)
            else:
                print("Error checking validator set")
                exit(2)
        else:
            alerter.run_continuous(args.interval)
            
    except Exception as e:
        logger.error(f"Valset stale alerter failed: {e}")
        exit(1)


if __name__ == "__main__":
    main()
