#!/usr/bin/env python3
"""
Validator Set Integrity Monitor

This component monitors validator set integrity by comparing TellorDataBridge contract
validator set parameters with Tellor Layer's canonical validator set data.

Features:
- Monitors validatorTimestamp changes in TellorDataBridge contract
- Queries Tellor Layer API when validator set changes are detected
- Compares timestamp, checkpoint, and power threshold between contract and Layer
- Detects integrity violations and API errors
- Sends Discord alerts for malicious activity or data mismatches
- Only performs integrity checks when validator set actually changes
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
from rpc_failover import EVMProviderPool, HttpEndpointPool

# logging will be configured in main() based on --verbose flag
logger = logging.getLogger(__name__)

class ValsetIntegrityMonitor:
    def __init__(self, disable_discord: bool = False, bridge_contract_address: Optional[str] = None,
                 ping_frequency_days: int = 7):
        """Initialize the validator set integrity monitor
        
        Args:
            disable_discord: If True, skip sending Discord alerts
            bridge_contract_address: Override TellorDataBridge contract address
            ping_frequency_days: Ping frequency in days (7=weekly, 1=daily, etc.)
        """
        try:
            self.config_manager = get_config_manager()
            
            # get configuration
            self.bridge_contract_address = bridge_contract_address or self.config_manager.get_bridge_contract()
            self.evm_rpc_urls = self.config_manager.get_evm_rpc_urls()
            self.layer_rpc_urls = self.config_manager.get_layer_rpc_urls()
            self.layer_chain = self.config_manager.get_layer_chain()
            self.evm_chain = self.config_manager.get_evm_chain()
            
            # store settings
            self.disable_discord = disable_discord
            self.ping_frequency_days = ping_frequency_days
            
            # initialize provider pools
            reset_minutes = self.config_manager.get_rpc_preference_reset_minutes()
            self.evm_pool = EVMProviderPool(self.evm_rpc_urls, request_timeout_s=15, preference_reset_minutes=reset_minutes)
            self.layer_pool = HttpEndpointPool(self.layer_rpc_urls, timeout_s=10, preference_reset_minutes=reset_minutes)

            # test EVM connection
            try:
                w3 = self.evm_pool.ensure_connected()
                latest_block = w3.eth.block_number
                logger.debug(f"Connected to EVM RPC. Latest block: {latest_block}")
            except Exception as e:
                raise ConnectionError(f"Failed to connect to any EVM RPC: {e}")
            
            # load bridge ABI
            self.bridge_abi = self._load_bridge_abi()
            
            # get Discord webhook URL for malicious activity
            self.discord_webhook_url = None if disable_discord else self._get_malicious_activity_discord_webhook()
            
            # set up timezone
            self.utc_tz = pytz.timezone('UTC')
            self.eastern_tz = pytz.timezone('US/Eastern')
            
            # state management for tracking last checked validator timestamp
            # put state file in data_dir/valset/
            data_dir = self.config_manager.get_data_dir()
            valset_dir = Path(data_dir) / "valset"
            valset_dir.mkdir(parents=True, exist_ok=True)
            self.state_file = valset_dir / "valset_integrity_state.json"
            self.last_checked_state = self._load_state()
            
            # initialize ping helper
            self.ping_helper = PingHelper(
                script_name="valset_integrity_monitor",
                data_dir=data_dir,
                discord_webhook_url=self.discord_webhook_url
            )
            
            logger.info(f"Initialized ValsetIntegrityMonitor for {self.layer_chain} → {self.evm_chain}")
            logger.info(f"Bridge contract: {self.bridge_contract_address}")
            logger.info(f"Layer RPC: {self.config_manager.get_layer_rpc_url()}")
            logger.info(f"State file: {self.state_file}")
            
            if self.disable_discord:
                logger.warning("Discord alerts: DISABLED via --no-discord flag")
            else:
                logger.info(f"Discord alerts: {'enabled' if self.discord_webhook_url else 'disabled (no webhook configured)'}")
            
        except Exception as e:
            logger.error(f"Failed to initialize ValsetIntegrityMonitor: {e}")
            raise
    
    def _load_bridge_abi(self):
        """Load the TellorDataBridge contract ABI"""
        try:
            abi_path = "abis/TellorDataBridge.json"
            with open(abi_path, 'r') as f:
                contract_data = json.load(f)
            logger.debug(f"Loaded TellorDataBridge ABI from {abi_path}")
            return contract_data['abi']
            
        except Exception as e:
            logger.error(f"Failed to load TellorDataBridge ABI: {e}")
            raise
    
    def _get_malicious_activity_discord_webhook(self) -> Optional[str]:
        """Get Discord webhook URL for malicious activity alerts"""
        try:
            # try to get from discord_webhooks configuration first
            webhook_url = self.config_manager.get_discord_webhook('malicious_activity')
            if webhook_url and webhook_url != "N/A":
                return webhook_url
            
            # fallback to general discord webhook
            return self.config_manager.get_discord_webhook_url()
            
        except Exception as e:
            logger.warning(f"Could not get Discord webhook URL: {e}")
            return None
    
    def _load_state(self) -> Dict[str, Any]:
        """Load last checked validator state from state file"""
        try:
            if self.state_file.exists():
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                logger.debug(f"Loaded integrity state from {self.state_file}")
                return state
            else:
                logger.info("No existing integrity state file, will initialize with current state")
                return {}
        except Exception as e:
            logger.warning(f"Failed to load integrity state: {e}, will initialize fresh")
            return {}
    
    def _save_state(self, validator_timestamp: int, last_checked_time: int):
        """Save current validator state to state file"""
        try:
            state = {
                "last_checked_validator_timestamp": validator_timestamp,
                "last_checked_time": last_checked_time
            }
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)
            logger.debug(f"Saved integrity state to {self.state_file}")
        except Exception as e:
            logger.error(f"Failed to save integrity state: {e}")
    
    def _get_current_validator_state(self) -> Optional[Dict[str, Any]]:
        """Get current validator state from the bridge contract"""
        try:
            # get current validator timestamp, checkpoint, and power threshold via provider pool
            def _call(contract):
                ts = contract.functions.validatorTimestamp().call()
                cp = contract.functions.lastValidatorSetCheckpoint().call()
                pt = contract.functions.powerThreshold().call()
                return ts, cp, pt

            ts, cp, pt = self.evm_pool.with_contract_call(
                address=self.bridge_contract_address,
                abi=self.bridge_abi,
                fn_builder=lambda c: _call(c)
            )

            return {
                "validator_timestamp": ts,
                "validator_set_checkpoint": cp.hex(),  # convert bytes32 to hex string
                "power_threshold": pt
            }
            
        except Exception as e:
            logger.error(f"Failed to get current validator state from contract: {e}")
            return None
    
    def _query_layer_validator_params(self, timestamp: int) -> Optional[Dict[str, Any]]:
        """Query Tellor Layer for validator checkpoint parameters
        
        Args:
            timestamp: Validator timestamp in milliseconds
            
        Returns:
            Dict with layer validator params or None if error
        """
        try:
            path = f"/layer/bridge/get_validator_checkpoint_params/{timestamp}"
            logger.debug(f"Querying Layer API path: {path}")
            data = self.layer_pool.get_json(path)
            
            # check for API error response
            if 'code' in data and 'message' in data:
                logger.error(f"Layer API error: code {data.get('code')}, message: {data.get('message')}")
                return {
                    'error': True,
                    'code': data.get('code'),
                    'message': data.get('message'),
                    'details': data.get('details', [])
                }
            
            # successful response
            logger.debug(f"Layer response: {data}")
            return {
                'error': False,
                'checkpoint': data.get('checkpoint'),
                'valset_hash': data.get('valset_hash'),
                'timestamp': data.get('timestamp'),
                'power_threshold': data.get('power_threshold')
            }
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to query Layer API: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Layer API response: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error querying Layer API: {e}")
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
        """Initialize state with current validator timestamp if no state exists"""
        if self.last_checked_state:
            logger.debug("State already exists, skipping initialization")
            return False
        
        logger.info("Initializing integrity state with current contract state")
        current_state = self._get_current_validator_state()
        if not current_state:
            logger.error("Failed to get current validator state for initialization")
            return False
        
        current_time = int(time.time() * 1000)  # current time in ms
        self._save_state(current_state["validator_timestamp"], current_time)
        self.last_checked_state = {
            "last_checked_validator_timestamp": current_state["validator_timestamp"],
            "last_checked_time": current_time
        }
        
        logger.info(f"Initialized with validator timestamp: {current_state['validator_timestamp']}")
        logger.info(f"Human readable: {self._format_timestamp(current_state['validator_timestamp'])}")
        
        return True
    
    def check_validator_integrity(self) -> Optional[Dict[str, Any]]:
        """Check validator set integrity by comparing contract state with Layer
        
        Returns:
            Dictionary with integrity check results or None if no check needed
        """
        try:
            current_state = self._get_current_validator_state()
            if not current_state:
                logger.error("Failed to get current validator state")
                return None
            
            # initialize state if this is the first run
            if not self.last_checked_state:
                self._initialize_state()
                return None  # don't check on initialization
            
            last_timestamp = self.last_checked_state.get("last_checked_validator_timestamp", 0)
            current_timestamp = current_state["validator_timestamp"]
            
            # only check integrity if validator timestamp has changed
            if current_timestamp == last_timestamp:
                logger.debug(f"No validator set change detected (timestamp: {current_timestamp})")
                return None
            
            logger.info(f"Validator set change detected, checking integrity...")
            logger.info(f"Previous timestamp: {last_timestamp}")
            logger.info(f"New timestamp: {current_timestamp}")
            
            # query Layer for validator parameters
            layer_params = self._query_layer_validator_params(current_timestamp)
            if not layer_params:
                logger.error("Failed to query Layer for validator parameters")
                # still update our state to avoid re-checking the same timestamp
                self._save_state(current_timestamp, int(time.time() * 1000))
                self.last_checked_state["last_checked_validator_timestamp"] = current_timestamp
                return {
                    'violation_type': 'layer_query_failed',
                    'contract_state': current_state,
                    'layer_params': None,
                    'error': 'Failed to query Layer API'
                }
            
            # check if Layer API returned an error
            if layer_params.get('error'):
                logger.error(f"Layer API returned error: {layer_params}")
                # update state to avoid re-checking
                self._save_state(current_timestamp, int(time.time() * 1000))
                self.last_checked_state["last_checked_validator_timestamp"] = current_timestamp
                return {
                    'violation_type': 'layer_api_error',
                    'contract_state': current_state,
                    'layer_params': layer_params,
                    'error': f"Code {layer_params.get('code')}: {layer_params.get('message')}"
                }
            
            # compare parameters
            integrity_violations = []
            
            # compare timestamp
            layer_timestamp = int(layer_params.get('timestamp', '0'))
            if current_timestamp != layer_timestamp:
                integrity_violations.append({
                    'param': 'timestamp',
                    'contract_value': current_timestamp,
                    'layer_value': layer_timestamp
                })
            
            # compare checkpoint
            contract_checkpoint = current_state["validator_set_checkpoint"]
            layer_checkpoint = layer_params.get('checkpoint', '')
            if contract_checkpoint.lower() != layer_checkpoint.lower():
                integrity_violations.append({
                    'param': 'checkpoint',
                    'contract_value': contract_checkpoint,
                    'layer_value': layer_checkpoint
                })
            
            # compare power threshold
            contract_power = current_state["power_threshold"]
            layer_power = int(layer_params.get('power_threshold', '0'))
            if contract_power != layer_power:
                integrity_violations.append({
                    'param': 'power_threshold',
                    'contract_value': contract_power,
                    'layer_value': layer_power
                })
            
            # update our state
            self._save_state(current_timestamp, int(time.time() * 1000))
            self.last_checked_state["last_checked_validator_timestamp"] = current_timestamp
            
            if integrity_violations:
                logger.error(f"Validator set integrity violations detected: {integrity_violations}")
                return {
                    'violation_type': 'parameter_mismatch',
                    'contract_state': current_state,
                    'layer_params': layer_params,
                    'violations': integrity_violations
                }
            else:
                logger.info("Validator set integrity verified - all parameters match")
                return {
                    'violation_type': None,
                    'contract_state': current_state,
                    'layer_params': layer_params,
                    'status': 'verified'
                }
                
        except Exception as e:
            logger.error(f"Error checking validator integrity: {e}")
            return None
    
    def generate_ping_content(self) -> str:
        """Generate ping content with current validator set information"""
        try:
            valset_info = self._get_current_validator_state()
            if not valset_info:
                return "**Status:** Unable to get validator set information"
            
            validator_timestamp = valset_info['validator_timestamp']
            formatted_timestamp = self.ping_helper.format_timestamp_et(validator_timestamp)
            
            ping_content = (
                f"**Current DataBridge Contract Validator Set:**\n"
                f"**Bridge Contract:** `{self.bridge_contract_address}`\n"
                f"**Validator Timestamp:** {validator_timestamp}\n"
                f"**Human Readable:** {formatted_timestamp}\n"
                f"**Checkpoint:** `{valset_info['validator_set_checkpoint']}`\n"
                f"**Power Threshold:** {valset_info['power_threshold']}"
            )
            
            return ping_content
            
        except Exception as e:
            logger.error(f"Failed to generate ping content: {e}")
            return f"**Status:** Error generating ping content: {e}"

    def send_discord_alert(self, integrity_result: Dict[str, Any]):
        """Send Discord alert for validator set integrity violations"""
        if self.disable_discord or not self.discord_webhook_url:
            return
        
        try:
            violation_type = integrity_result.get('violation_type')
            contract_state = integrity_result.get('contract_state', {})
            layer_params = integrity_result.get('layer_params', {})
            
            timestamp = contract_state.get('validator_timestamp', 0)
            time_str = self._format_timestamp(timestamp)
            
            if violation_type == 'layer_query_failed':
                alert_message = (
                    f"🚨 **Validator Set Integrity Check Failed** - {self.layer_chain} → {self.evm_chain}\n\n"
                    f"**Error:** Failed to query Tellor Layer API\n"
                    f"**Timestamp:** {timestamp}\n"
                    f"**Human Readable:** {time_str}\n"
                    f"**Bridge Contract:** `{self.bridge_contract_address}`\n\n"
                    f"**Details:** Unable to retrieve validator parameters from Layer to verify integrity"
                )
            elif violation_type == 'layer_api_error':
                error_msg = integrity_result.get('error', 'Unknown error')
                alert_message = (
                    f"🚨 **Tellor Layer API Error** - {self.layer_chain} → {self.evm_chain}\n\n"
                    f"**Error:** {error_msg}\n"
                    f"**Timestamp:** {timestamp}\n"
                    f"**Human Readable:** {time_str}\n"
                    f"**Bridge Contract:** `{self.bridge_contract_address}`\n\n"
                    f"**Details:** Layer API returned error when querying validator checkpoint parameters"
                )
            elif violation_type == 'parameter_mismatch':
                violations = integrity_result.get('violations', [])
                violations_text = ""
                for v in violations:
                    violations_text += f"**{v['param'].title()}:**\n"
                    violations_text += f"- Contract: `{v['contract_value']}`\n"
                    violations_text += f"- Layer: `{v['layer_value']}`\n\n"
                
                alert_message = (
                    f"🚨 **Validator Set Integrity VIOLATION** - {self.layer_chain} → {self.evm_chain}\n\n"
                    f"**Timestamp:** {timestamp}\n"
                    f"**Human Readable:** {time_str}\n"
                    f"**Bridge Contract:** `{self.bridge_contract_address}`\n\n"
                    f"**Mismatched Parameters:**\n{violations_text}"
                    f"**This indicates potential malicious activity or data corruption!**"
                )
            else:
                # shouldn't happen, but just in case
                alert_message = (
                    f"🚨 **Unknown Validator Set Issue** - {self.layer_chain} → {self.evm_chain}\n\n"
                    f"**Timestamp:** {timestamp}\n"
                    f"**Human Readable:** {time_str}\n"
                    f"**Bridge Contract:** `{self.bridge_contract_address}`"
                )
            
            payload = {
                "content": alert_message,
                "username": "Valset Integrity Monitor"
            }
            
            response = requests.post(self.discord_webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            
            logger.info(f"Sent Discord alert for validator set integrity issue: {violation_type}")
            
        except Exception as e:
            logger.error(f"Failed to send Discord alert: {e}")
    
    def run_once(self, send_ping: bool = False) -> Optional[Dict[str, Any]]:
        """Run validator set integrity check once and return results"""
        logger.info("Checking validator set integrity...")
        
        integrity_result = self.check_validator_integrity()
        
        if integrity_result:
            violation_type = integrity_result.get('violation_type')
            if violation_type:
                logger.warning(f"Validator set integrity issue detected: {violation_type}")
                self.send_discord_alert(integrity_result)
            else:
                logger.info("Validator set integrity verified")
        else:
            logger.debug("No validator set changes detected, no integrity check needed")
        
        # check for scheduled ping
        if self.ping_helper.should_send_ping(self.ping_frequency_days) or send_ping:
            ping_content = self.generate_ping_content()
            self.ping_helper.send_ping(ping_content, self.ping_frequency_days, force=send_ping)
        
        return integrity_result
    
    def run_continuous(self, interval_minutes: int = 5):
        """Run continuous integrity monitoring with specified interval"""
        interval_seconds = interval_minutes * 60
        
        logger.info(f"Starting continuous validator set integrity monitoring (interval: {interval_minutes}m)")
        
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
    """Main entry point for the validator set integrity monitor"""
    parser = argparse.ArgumentParser(description='Monitor TellorDataBridge validator set integrity against Tellor Layer')
    parser.add_argument('--once', action='store_true', help='Run once instead of continuously')
    parser.add_argument('--no-discord', action='store_true', help='Disable Discord alerts')
    parser.add_argument('--bridge-address', type=str, help='Override TellorDataBridge contract address')
    parser.add_argument('--interval', type=int, default=5, help='Monitoring interval in minutes (default: 5)')
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
        monitor = ValsetIntegrityMonitor(
            disable_discord=args.no_discord,
            bridge_contract_address=args.bridge_address,
            ping_frequency_days=args.ping_frequency
        )
        
        if args.once:
            # run once
            integrity_result = monitor.run_once(send_ping=args.ping_now)
            if integrity_result and integrity_result.get('violation_type'):
                exit(1)  # exit with error code if violations detected
        else:
            # run continuously
            monitor.run_continuous(interval_minutes=args.interval)
            
    except Exception as e:
        logger.error(f"Monitor failed: {e}")
        exit(1)

if __name__ == "__main__":
    main() 