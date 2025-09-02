#!/usr/bin/env python3
"""
Validator Set Stale Alerter

This component monitors TellorDataBridge contracts for stale validator sets
by checking if the validatorTimestamp is older than a configured threshold.

Features:
- Monitors validatorTimestamp for staleness
- Configurable staleness threshold
- Sends Discord alerts for stale validator sets
- Shows both millisecond and human-readable timestamps
- Weekly ping functionality
"""

import time
import requests
import logging
import json
import argparse
from datetime import datetime
from typing import Optional, Dict, Any
from web3 import Web3
import pytz
from pathlib import Path
from config_manager import get_config_manager
from logger_utils import setup_logging
from ping_helper import PingHelper

# logging will be configured in main() based on --verbose flag
logger = logging.getLogger(__name__)

class ValsetStaleAlerter:
    def __init__(self, disable_discord: bool = False, bridge_contract_address: Optional[str] = None, 
                 ping_frequency_days: int = 7):
        """Initialize the validator set stale alerter
        
        Args:
            disable_discord: If True, skip sending Discord alerts
            bridge_contract_address: Override TellorDataBridge contract address
            ping_frequency_days: Ping frequency in days (7=weekly, 1=daily, etc.)
        """
        try:
            self.config_manager = get_config_manager()
            
            # get configuration
            self.bridge_contract_address = bridge_contract_address or self.config_manager.get_bridge_contract()
            self.evm_rpc_url = self.config_manager.get_evm_rpc_url()
            self.layer_chain = self.config_manager.get_layer_chain()
            self.evm_chain = self.config_manager.get_evm_chain()
            
            # store settings
            self.disable_discord = disable_discord
            self.ping_frequency_days = ping_frequency_days
            
            # staleness threshold (2 weeks by default)
            self.staleness_threshold_hours = 24 * 14
            
            # initialize Web3
            self.w3 = Web3(Web3.HTTPProvider(self.evm_rpc_url))
            
            # test connection
            try:
                latest_block = self.w3.eth.block_number
                logger.debug(f"Connected to EVM RPC. Latest block: {latest_block}")
            except Exception as e:
                raise ConnectionError(f"Failed to connect to EVM RPC: {e}")
            
            # load bridge contract
            self.bridge_contract = self._load_bridge_contract()
            
            # get Discord webhook URL
            self.discord_webhook_url = None if disable_discord else self._get_valset_discord_webhook()
            
            # set up timezone
            self.eastern_tz = pytz.timezone('US/Eastern')
            
            # initialize ping helper
            data_dir = self.config_manager.get_data_dir()
            self.ping_helper = PingHelper(
                script_name="valset_stale_alerter",
                data_dir=data_dir,
                discord_webhook_url=self.discord_webhook_url
            )
            
            logger.info(f"Initialized ValsetStaleAlerter for {self.layer_chain} → {self.evm_chain}")
            logger.info(f"Bridge contract: {self.bridge_contract_address}")
            logger.info(f"Staleness threshold: {self.staleness_threshold_hours} hours")
            
            if self.disable_discord:
                logger.warning("Discord alerts: DISABLED via --no-discord flag")
            else:
                logger.info(f"Discord alerts: {'enabled' if self.discord_webhook_url else 'disabled (no webhook configured)'}")
            
        except Exception as e:
            logger.error(f"Failed to initialize ValsetStaleAlerter: {e}")
            raise
    
    def _load_bridge_contract(self):
        """Load the TellorDataBridge contract ABI and create Web3 contract instance"""
        try:
            abi_path = "abis/TellorDataBridge.json"
            with open(abi_path, 'r') as f:
                contract_data = json.load(f)
            
            contract = self.w3.eth.contract(
                address=self.bridge_contract_address,
                abi=contract_data['abi']
            )
            
            logger.debug(f"Loaded TellorDataBridge contract from {abi_path}")
            return contract
            
        except Exception as e:
            logger.error(f"Failed to load TellorDataBridge contract: {e}")
            raise
    
    def _get_valset_discord_webhook(self) -> Optional[str]:
        """Get Discord webhook URL for validator set alerts"""
        try:
            # try to get specific valset_stale webhook first
            webhook_url = self.config_manager.get_discord_webhook('valset_stale')
            if webhook_url and webhook_url != "N/A":
                return webhook_url
            
            # fallback to general discord webhook
            return self.config_manager.get_discord_webhook_url()
            
        except Exception as e:
            logger.warning(f"Could not get Discord webhook URL: {e}")
            return None
    
    def _format_timestamp(self, timestamp_ms: int) -> str:
        """Format timestamp in milliseconds to human-readable ET string"""
        return self.ping_helper.format_timestamp_et(timestamp_ms)
    
    def get_validator_set_info(self) -> Optional[Dict[str, Any]]:
        """Get current validator set information from bridge contract"""
        try:
            validator_timestamp = self.bridge_contract.functions.validatorTimestamp().call()
            validator_set_checkpoint = self.bridge_contract.functions.lastValidatorSetCheckpoint().call()
            power_threshold = self.bridge_contract.functions.powerThreshold().call()
            
            return {
                'validator_timestamp': validator_timestamp,
                'validator_set_checkpoint': validator_set_checkpoint.hex(),
                'power_threshold': power_threshold,
                'formatted_timestamp': self._format_timestamp(validator_timestamp)
            }
            
        except Exception as e:
            logger.error(f"Failed to get validator set info: {e}")
            return None
    
    def check_valset_staleness(self) -> Optional[Dict[str, Any]]:
        """Check if validator set is stale"""
        try:
            valset_info = self.get_validator_set_info()
            if not valset_info:
                return None
            
            validator_timestamp = valset_info['validator_timestamp']
            current_time_ms = int(time.time() * 1000)
            
            # calculate age in hours
            age_ms = current_time_ms - validator_timestamp
            age_hours = age_ms / (1000 * 60 * 60)
            
            if age_hours > self.staleness_threshold_hours:
                logger.warning(f"Validator set is stale: {age_hours:.1f}h old (threshold: {self.staleness_threshold_hours}h)")
                return {
                    'is_stale': True,
                    'age_hours': age_hours,
                    'threshold_hours': self.staleness_threshold_hours,
                    'valset_info': valset_info
                }
            else:
                logger.debug(f"Validator set is fresh: {age_hours:.1f}h old")
                return {
                    'is_stale': False,
                    'age_hours': age_hours,
                    'threshold_hours': self.staleness_threshold_hours,
                    'valset_info': valset_info
                }
                
        except Exception as e:
            logger.error(f"Error checking validator set staleness: {e}")
            return None
    
    def send_discord_alert(self, staleness_result: Dict[str, Any]):
        """Send Discord alert for stale validator set"""
        if self.disable_discord or not self.discord_webhook_url:
            return
        
        try:
            valset_info = staleness_result['valset_info']
            age_hours = staleness_result['age_hours']
            threshold_hours = staleness_result['threshold_hours']
            
            validator_timestamp = valset_info['validator_timestamp']
            formatted_timestamp = valset_info['formatted_timestamp']
            
            # create alert message
            alert_message = (
                f"⏰ **Validator Set Stale Alert** - {self.layer_chain} → {self.evm_chain}\n\n"
                f"**Validator Set Age:** {age_hours:.1f} hours (threshold: {threshold_hours}h)\n"
                f"**Validator Timestamp:** {validator_timestamp}\n"
                f"**Human Readable:** {formatted_timestamp}\n"
                f"**Bridge Contract:** `{self.bridge_contract_address}`\n"
                f"**Checkpoint:** `{valset_info['validator_set_checkpoint']}`\n"
                f"**Power Threshold:** {valset_info['power_threshold']}"
            )
            
            payload = {
                "content": alert_message,
                "username": "Valset Stale Alerter"
            }
            
            response = requests.post(self.discord_webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            
            logger.info("Sent Discord alert for stale validator set")
            
        except Exception as e:
            logger.error(f"Failed to send Discord alert: {e}")
    
    def generate_ping_content(self) -> str:
        """Generate ping content with current validator set information"""
        try:
            valset_info = self.get_validator_set_info()
            if not valset_info:
                return "**Status:** Unable to get validator set information"
            
            validator_timestamp = valset_info['validator_timestamp']
            formatted_timestamp = valset_info['formatted_timestamp']
            
            ping_content = (
                f"**Current DataBridge Contract Validator Set:**\n"
                f"**Bridge Contract:** `{self.bridge_contract_address}`\n"
                f"**Validator Timestamp:** {validator_timestamp}\n"
                f"**Human Readable:** {formatted_timestamp}\n"
                f"**Checkpoint:** `{valset_info['validator_set_checkpoint']}`\n"
                f"**Power Threshold:** {valset_info['power_threshold']}\n"
                f"**Staleness Threshold:** {self.staleness_threshold_hours} hours"
            )
            
            return ping_content
            
        except Exception as e:
            logger.error(f"Failed to generate ping content: {e}")
            return f"**Status:** Error generating ping content: {e}"
    
    def run_once(self, send_ping: bool = False) -> Optional[Dict[str, Any]]:
        """Run validator set staleness check once"""
        logger.info("Checking validator set staleness...")
        
        # check for staleness
        staleness_result = self.check_valset_staleness()
        
        if staleness_result and staleness_result['is_stale']:
            logger.warning("Stale validator set detected")
            self.send_discord_alert(staleness_result)
        elif staleness_result:
            logger.debug("Validator set is not stale")
        
        # check for scheduled ping
        if self.ping_helper.should_send_ping(self.ping_frequency_days) or send_ping:
            ping_content = self.generate_ping_content()
            self.ping_helper.send_ping(ping_content, self.ping_frequency_days, force=send_ping)
        
        return staleness_result
    
    def run_continuous(self, interval_minutes: int = 60):
        """Run continuous monitoring with specified interval"""
        interval_seconds = interval_minutes * 60
        
        logger.info(f"Starting continuous validator set staleness monitoring (interval: {interval_minutes}m)")
        
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
    """Main entry point for the validator set stale alerter"""
    parser = argparse.ArgumentParser(description='Monitor TellorDataBridge validator set for staleness')
    parser.add_argument('--once', action='store_true', help='Run once instead of continuously')
    parser.add_argument('--no-discord', action='store_true', help='Disable Discord alerts')
    parser.add_argument('--bridge-address', type=str, help='Override TellorDataBridge contract address')
    parser.add_argument('--interval', type=int, default=60, help='Monitoring interval in minutes (default: 60)')
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
        # initialize alerter
        alerter = ValsetStaleAlerter(
            disable_discord=args.no_discord,
            bridge_contract_address=args.bridge_address,
            ping_frequency_days=args.ping_frequency
        )
        
        if args.once:
            # run once
            result = alerter.run_once(send_ping=args.ping_now)
            if result:
                print(f"\nValidator Set Status:")
                print(f"  Timestamp: {result['valset_info']['validator_timestamp']}")
                print(f"  Age: {result['age_hours']:.1f} hours")
                print(f"  Stale: {result['is_stale']}")
        else:
            # run continuously
            alerter.run_continuous(interval_minutes=args.interval)
            
    except Exception as e:
        logger.error(f"Alerter failed: {e}")
        exit(1)

if __name__ == "__main__":
    main()
