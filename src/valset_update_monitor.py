#!/usr/bin/env python3
"""
Validator Set Update Monitor

This component monitors validator set updates in TellorDataBridge contracts by tracking
changes to the validatorTimestamp and alerting when updates occur.

Features:
- Monitors validatorTimestamp in TellorDataBridge contract
- Tracks last seen timestamp in state file
- Detects validator set updates and guardian resets
- Sends Discord alerts with validator set details
- Shows timestamps in both raw milliseconds and human-readable format
- Simple and self-contained monitoring
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

# logging will be configured in main() based on --verbose flag
logger = logging.getLogger(__name__)

class ValsetUpdateMonitor:
    def __init__(self, disable_discord: bool = False, bridge_contract_address: Optional[str] = None):
        """Initialize the validator set update monitor
        
        Args:
            disable_discord: If True, skip sending Discord alerts
            bridge_contract_address: Override TellorDataBridge contract address
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
            self.discord_webhook_url = None if disable_discord else self._get_valset_updates_discord_webhook()
            
            # set up timezone
            self.utc_tz = pytz.timezone('UTC')
            self.eastern_tz = pytz.timezone('US/Eastern')
            
            # state management for tracking last seen validator timestamp
            # put state file in data_dir/valset/
            data_dir = self.config_manager.get_data_dir()
            valset_dir = Path(data_dir) / "valset"
            valset_dir.mkdir(parents=True, exist_ok=True)
            self.state_file = valset_dir / "last_valset_timestamp.json"
            self.last_validator_state = self._load_state()
            
            logger.info(f"Initialized ValsetUpdateMonitor for {self.layer_chain} → {self.evm_chain}")
            logger.info(f"Bridge contract: {self.bridge_contract_address}")
            logger.info(f"State file: {self.state_file}")
            
            if self.disable_discord:
                logger.warning("Discord alerts: DISABLED via --no-discord flag")
            else:
                logger.info(f"Discord alerts: {'enabled' if self.discord_webhook_url else 'disabled (no webhook configured)'}")
            
        except Exception as e:
            logger.error(f"Failed to initialize ValsetUpdateMonitor: {e}")
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
    
    def _get_valset_updates_discord_webhook(self) -> Optional[str]:
        """Get Discord webhook URL for validator set update alerts"""
        try:
            # try to get from discord_webhooks configuration first
            webhooks = self.config_manager.get_discord_webhooks()
            if webhooks and 'valset_updates' in webhooks:
                return webhooks['valset_updates']
            
            # fallback to general discord webhook
            return self.config_manager.get_discord_webhook_url()
            
        except Exception as e:
            logger.warning(f"Could not get Discord webhook URL: {e}")
            return None
    
    def _load_state(self) -> Dict[str, Any]:
        """Load last validator state from state file"""
        try:
            if self.state_file.exists():
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                logger.debug(f"Loaded validator state from {self.state_file}")
                return state
            else:
                logger.info("No existing validator state file, will initialize with current state")
                return {}
        except Exception as e:
            logger.warning(f"Failed to load validator state: {e}, will initialize fresh")
            return {}
    
    def _save_state(self, validator_timestamp: int, validator_set_checkpoint: str, power_threshold: int):
        """Save current validator state to state file"""
        try:
            state = {
                "validator_timestamp": validator_timestamp,
                "validator_set_checkpoint": validator_set_checkpoint,
                "power_threshold": power_threshold,
                "last_updated": int(time.time() * 1000)  # current time in ms
            }
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)
            logger.debug(f"Saved validator state to {self.state_file}")
        except Exception as e:
            logger.error(f"Failed to save validator state: {e}")
    
    def _get_current_validator_state(self) -> Optional[Dict[str, Any]]:
        """Get current validator state from the bridge contract"""
        try:
            # get current validator timestamp, checkpoint, and power threshold
            validator_timestamp = self.bridge_contract.functions.validatorTimestamp().call()
            validator_set_checkpoint = self.bridge_contract.functions.lastValidatorSetCheckpoint().call()
            power_threshold = self.bridge_contract.functions.powerThreshold().call()
            
            return {
                "validator_timestamp": validator_timestamp,
                "validator_set_checkpoint": validator_set_checkpoint.hex(),  # convert bytes32 to hex string
                "power_threshold": power_threshold
            }
            
        except Exception as e:
            logger.error(f"Failed to get current validator state from contract: {e}")
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
    
    def _initialize_state(self) -> bool:
        """Initialize state with current validator set if no state exists"""
        if self.last_validator_state:
            logger.debug("State already exists, skipping initialization")
            return False
        
        logger.info("Initializing validator state with current contract state")
        current_state = self._get_current_validator_state()
        if not current_state:
            logger.error("Failed to get current validator state for initialization")
            return False
        
        self._save_state(
            current_state["validator_timestamp"],
            current_state["validator_set_checkpoint"], 
            current_state["power_threshold"]
        )
        self.last_validator_state = current_state
        
        logger.info(f"Initialized with validator timestamp: {current_state['validator_timestamp']}")
        logger.info(f"Human readable: {self._format_timestamp(current_state['validator_timestamp'])}")
        
        return True
    
    def check_for_validator_update(self) -> Optional[Dict[str, Any]]:
        """Check if validator set has been updated
        
        Returns:
            Dictionary with update info if update detected, None if no update
        """
        try:
            current_state = self._get_current_validator_state()
            if not current_state:
                logger.error("Failed to get current validator state")
                return None
            
            # initialize state if this is the first run
            if not self.last_validator_state:
                self._initialize_state()
                return None  # don't alert on initialization
            
            last_timestamp = self.last_validator_state.get("validator_timestamp", 0)
            current_timestamp = current_state["validator_timestamp"]
            
            # check if validator timestamp has changed
            if current_timestamp != last_timestamp:
                logger.info(f"Validator set update detected!")
                logger.info(f"Previous timestamp: {last_timestamp}")
                logger.info(f"New timestamp: {current_timestamp}")
                
                update_info = {
                    "previous_timestamp": last_timestamp,
                    "new_timestamp": current_timestamp,
                    "validator_set_checkpoint": current_state["validator_set_checkpoint"],
                    "power_threshold": current_state["power_threshold"],
                    "previous_checkpoint": self.last_validator_state.get("validator_set_checkpoint", "unknown"),
                    "previous_power_threshold": self.last_validator_state.get("power_threshold", 0)
                }
                
                # update our state
                self._save_state(
                    current_timestamp,
                    current_state["validator_set_checkpoint"],
                    current_state["power_threshold"]
                )
                self.last_validator_state = current_state.copy()
                
                return update_info
            else:
                logger.debug(f"No validator set update detected (timestamp: {current_timestamp})")
                return None
                
        except Exception as e:
            logger.error(f"Error checking for validator update: {e}")
            return None
    
    def send_discord_alert(self, update_info: Dict[str, Any]):
        """Send Discord alert for validator set update"""
        if self.disable_discord or not self.discord_webhook_url:
            return
        
        try:
            new_timestamp = update_info["new_timestamp"]
            previous_timestamp = update_info["previous_timestamp"]
            checkpoint = update_info["validator_set_checkpoint"]
            power_threshold = update_info["power_threshold"]
            
            # format timestamps
            new_time_str = self._format_timestamp(new_timestamp)
            previous_time_str = self._format_timestamp(previous_timestamp) if previous_timestamp > 0 else "N/A"
            
            # create alert message
            alert_message = (
                f"🔄 **Validator Set Updated** - {self.layer_chain} → {self.evm_chain}\n\n"
                f"**New Timestamp:** {new_timestamp}\n"
                f"**Human Readable:** {new_time_str}\n"
                f"**Previous Timestamp:** {previous_timestamp}\n"
                f"**Previous Human Readable:** {previous_time_str}\n\n"
                f"**Validator Set Checkpoint:** `{checkpoint}`\n"
                f"**Power Threshold:** {power_threshold}\n"
                f"**Bridge Contract:** `{self.bridge_contract_address}`"
            )
            
            payload = {
                "content": alert_message,
                "username": "Validator Set Monitor"
            }
            
            response = requests.post(self.discord_webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            
            logger.info("Sent Discord alert for validator set update")
            
        except Exception as e:
            logger.error(f"Failed to send Discord alert: {e}")
    
    def run_once(self) -> Optional[Dict[str, Any]]:
        """Run validator set check once and return results"""
        logger.info("Checking for validator set updates...")
        
        update_info = self.check_for_validator_update()
        
        if update_info:
            logger.info("Validator set update detected")
            self.send_discord_alert(update_info)
        else:
            logger.debug("No validator set updates detected")
        
        return update_info
    
    def run_continuous(self, interval_minutes: int = 5):
        """Run continuous monitoring with specified interval"""
        interval_seconds = interval_minutes * 60
        
        logger.info(f"Starting continuous validator set monitoring (interval: {interval_minutes}m)")
        
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
    """Main entry point for the validator set update monitor"""
    parser = argparse.ArgumentParser(description='Monitor TellorDataBridge validator set updates')
    parser.add_argument('--once', action='store_true', help='Run once instead of continuously')
    parser.add_argument('--no-discord', action='store_true', help='Disable Discord alerts')
    parser.add_argument('--bridge-address', type=str, help='Override TellorDataBridge contract address')
    parser.add_argument('--interval', type=int, default=5, help='Monitoring interval in minutes (default: 5)')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose debug logging')
    
    args = parser.parse_args()
    
    # setup logging based on verbose flag
    setup_logging(verbose=args.verbose)
    
    try:
        # initialize monitor
        monitor = ValsetUpdateMonitor(
            disable_discord=args.no_discord,
            bridge_contract_address=args.bridge_address
        )
        
        if args.once:
            # run once
            update_info = monitor.run_once()
            if update_info:
                exit(0)  # successful detection
        else:
            # run continuously
            monitor.run_continuous(interval_minutes=args.interval)
            
    except Exception as e:
        logger.error(f"Monitor failed: {e}")
        exit(1)

if __name__ == "__main__":
    main() 