#!/usr/bin/env python3
"""
Adaptor Guardian Monitor

This component monitors GuardedLiquityV2OracleAdaptor contracts for guardian management
and pause/unpause events, sending Discord alerts when state changes occur.

Features:
- Monitors multiple adaptor contracts for guardian and pause events
- Uses event log filtering to avoid missing events between intervals
- Queries contract name and project for context in alerts
- Sends Discord alerts with full contract and event details
- Tracks last processed block to ensure no events are missed
- Simple and self-contained monitoring
"""

import time
import requests
import logging
import json
import argparse
from datetime import datetime
from typing import Optional, Dict, Any, List
from web3 import Web3
import pytz
from pathlib import Path
from config_manager import get_config_manager
from logger_utils import setup_logging

# logging will be configured in main() based on --verbose flag
logger = logging.getLogger(__name__)

class AdaptorGuardianMonitor:
    def __init__(self, disable_discord: bool = False):
        """Initialize the adaptor guardian monitor
        
        Args:
            disable_discord: If True, skip sending Discord alerts
        """
        try:
            self.config_manager = get_config_manager()
            
            # get configuration
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
            
            # load adaptor contracts configuration
            self.adaptor_contracts = self._load_adaptor_contracts()
            
            # load contract ABI and create contract instances
            self.contract_instances = self._load_contract_instances()
            
            # get Discord webhook URL
            self.discord_webhook_url = None if disable_discord else self._get_guardian_discord_webhook()
            
            # set up timezone
            self.utc_tz = pytz.timezone('UTC')
            self.eastern_tz = pytz.timezone('US/Eastern')
            
            # state management for tracking last processed block
            # put state file in data_dir/validation/
            data_dir = self.config_manager.get_data_dir()
            validation_dir = Path(data_dir) / "validation"
            validation_dir.mkdir(parents=True, exist_ok=True)
            self.state_file = validation_dir / "adaptor_guardian_state.json"
            self.last_processed_block = self._load_state()
            
            logger.info(f"Initialized AdaptorGuardianMonitor for {self.layer_chain} → {self.evm_chain}")
            logger.info(f"Monitoring {len(self.adaptor_contracts)} adaptor contracts")
            logger.info(f"State file: {self.state_file}")
            
            if self.disable_discord:
                logger.warning("Discord alerts: DISABLED via --no-discord flag")
            else:
                logger.info(f"Discord alerts: {'enabled' if self.discord_webhook_url else 'disabled (no webhook configured)'}")
            
        except Exception as e:
            logger.error(f"Failed to initialize AdaptorGuardianMonitor: {e}")
            raise
    
    def _load_adaptor_contracts(self) -> List[Dict[str, str]]:
        """Load adaptor contract addresses from adaptor_contracts.json"""
        try:
            with open('adaptor_contracts.json', 'r') as f:
                data = json.load(f)
            
            contracts = data.get('contracts', [])
            logger.debug(f"Loaded {len(contracts)} adaptor contracts from adaptor_contracts.json")
            return contracts
            
        except Exception as e:
            logger.error(f"Failed to load adaptor contracts: {e}")
            raise
    
    def _load_contract_instances(self) -> Dict[str, Any]:
        """Load contract ABI and create Web3 contract instances"""
        try:
            abi_path = "abis/GuardedLiquityV2OracleAdaptor.json"
            with open(abi_path, 'r') as f:
                contract_data = json.load(f)
            
            instances = {}
            for contract_info in self.adaptor_contracts:
                address = contract_info['address']
                instances[address] = self.w3.eth.contract(
                    address=address,
                    abi=contract_data['abi']
                )
            
            logger.debug(f"Loaded {len(instances)} contract instances from {abi_path}")
            return instances
            
        except Exception as e:
            logger.error(f"Failed to load contract instances: {e}")
            raise
    
    def _get_guardian_discord_webhook(self) -> Optional[str]:
        """Get Discord webhook URL for guardian alerts"""
        try:
            # try to get specific guardian webhook first
            webhook_url = self.config_manager.get_discord_webhook('malicious_activity')
            if webhook_url and webhook_url != "N/A":
                return webhook_url
            
            # fallback to general discord webhook
            return self.config_manager.get_discord_webhook_url()
            
        except Exception as e:
            logger.warning(f"Could not get Discord webhook URL: {e}")
            return None
    
    def _load_state(self) -> int:
        """Load last processed block from state file"""
        try:
            if self.state_file.exists():
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                block_number = state.get('last_processed_block', 0)
                logger.debug(f"Loaded state: last processed block {block_number}")
                return block_number
            else:
                # start from current block if no state exists
                current_block = self.w3.eth.block_number
                logger.info(f"No existing state file, starting from current block: {current_block}")
                return current_block
        except Exception as e:
            logger.warning(f"Failed to load state: {e}, starting from current block")
            return self.w3.eth.block_number
    
    def _save_state(self, block_number: int):
        """Save last processed block to state file"""
        try:
            state = {
                "last_processed_block": block_number,
                "last_updated": int(time.time())
            }
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)
            logger.debug(f"Saved state: last processed block {block_number}")
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
    
    def _get_contract_info(self, contract_address: str) -> Dict[str, str]:
        """Get name and project info from contract"""
        try:
            contract = self.contract_instances[contract_address]
            
            name = contract.functions.name().call()
            project = contract.functions.project().call()
            
            # get description from config
            description = "Unknown"
            for contract_info in self.adaptor_contracts:
                if contract_info['address'].lower() == contract_address.lower():
                    description = contract_info['description']
                    break
            
            return {
                'name': name,
                'project': project,
                'description': description
            }
            
        except Exception as e:
            logger.error(f"Failed to get contract info for {contract_address}: {e}")
            return {
                'name': 'Unknown',
                'project': 'Unknown',
                'description': 'Unknown'
            }
    
    def _format_timestamp(self, timestamp: int) -> str:
        """Format timestamp to human-readable string"""
        dt = datetime.fromtimestamp(timestamp, tz=self.utc_tz)
        utc_str = dt.strftime('%Y-%m-%d %H:%M:%S UTC')
        eastern_dt = dt.astimezone(self.eastern_tz)
        eastern_str = eastern_dt.strftime('%Y-%m-%d %H:%M:%S %Z')
        
        return f"{utc_str} ({eastern_str})"
    
    def _get_events_for_contract(self, contract_address: str, from_block: int, to_block: int) -> List[Dict[str, Any]]:
        """Get all guardian/pause events for a specific contract in block range"""
        try:
            contract = self.contract_instances[contract_address]
            events = []
            
            # event names and their signatures
            event_filters = [
                ('GuardianAdded', contract.events.GuardianAdded),
                ('GuardianRemoved', contract.events.GuardianRemoved),
                ('AdminRemoved', contract.events.AdminRemoved),
                ('Paused', contract.events.Paused),
                ('Unpaused', contract.events.Unpaused)
            ]
            
            for event_name, event_filter in event_filters:
                try:
                    logs = event_filter.getLogs(fromBlock=from_block, toBlock=to_block)
                    
                    for log in logs:
                        event_data = {
                            'contract_address': contract_address,
                            'event_name': event_name,
                            'block_number': log['blockNumber'],
                            'transaction_hash': log['transactionHash'].hex(),
                            'args': dict(log['args']) if log['args'] else {}
                        }
                        
                        # get block timestamp
                        try:
                            block = self.w3.eth.get_block(log['blockNumber'])
                            event_data['timestamp'] = block['timestamp']
                        except Exception as e:
                            logger.warning(f"Failed to get block timestamp for block {log['blockNumber']}: {e}")
                            event_data['timestamp'] = int(time.time())
                        
                        events.append(event_data)
                        
                except Exception as e:
                    logger.warning(f"Failed to get {event_name} events for {contract_address}: {e}")
                    continue
            
            # sort events by block number and transaction index
            events.sort(key=lambda x: (x['block_number'], x['transaction_hash']))
            
            return events
            
        except Exception as e:
            logger.error(f"Failed to get events for contract {contract_address}: {e}")
            return []
    
    def check_guardian_events(self) -> List[Dict[str, Any]]:
        """Check for new guardian and pause events across all contracts"""
        try:
            current_block = self.w3.eth.block_number
            from_block = self.last_processed_block + 1
            
            if from_block > current_block:
                logger.debug("No new blocks to process")
                return []
            
            logger.info(f"Checking for events from block {from_block} to {current_block}")
            
            all_events = []
            
            for contract_info in self.adaptor_contracts:
                contract_address = contract_info['address']
                
                logger.debug(f"Checking events for contract {contract_address}")
                events = self._get_events_for_contract(contract_address, from_block, current_block)
                
                if events:
                    logger.info(f"Found {len(events)} events for {contract_address}")
                    all_events.extend(events)
            
            # update state
            self._save_state(current_block)
            self.last_processed_block = current_block
            
            return all_events
            
        except Exception as e:
            logger.error(f"Failed to check guardian events: {e}")
            return []
    
    def send_discord_alert(self, event: Dict[str, Any]):
        """Send Discord alert for guardian/pause event"""
        if self.disable_discord or not self.discord_webhook_url:
            return
        
        try:
            contract_address = event['contract_address']
            event_name = event['event_name']
            timestamp = event['timestamp']
            block_number = event['block_number']
            tx_hash = event['transaction_hash']
            args = event['args']
            
            # get contract info
            contract_info = self._get_contract_info(contract_address)
            
            # format timestamp
            time_str = self._format_timestamp(timestamp)
            
            # create event-specific message
            if event_name == 'GuardianAdded':
                guardian_address = args.get('guardian', 'Unknown')
                event_message = f"**Guardian Added**\n**Guardian Address:** `{guardian_address}`"
                alert_icon = "🔐"
            elif event_name == 'GuardianRemoved':
                guardian_address = args.get('guardian', 'Unknown')
                event_message = f"**Guardian Removed**\n**Guardian Address:** `{guardian_address}`"
                alert_icon = "🚫"
            elif event_name == 'AdminRemoved':
                event_message = f"**Admin Removed**\n**Note:** Admin privileges have been permanently removed"
                alert_icon = "⚠️"
            elif event_name == 'Paused':
                event_message = f"**Contract Paused**\n**Note:** Oracle reads are now disabled"
                alert_icon = "⏸️"
            elif event_name == 'Unpaused':
                event_message = f"**Contract Unpaused**\n**Note:** Oracle reads are now enabled"
                alert_icon = "▶️"
            else:
                event_message = f"**{event_name}**"
                alert_icon = "📢"
            
            # create full alert message
            alert_message = (
                f"{alert_icon} **Adaptor Guardian Event** - {self.layer_chain} → {self.evm_chain}\n\n"
                f"{event_message}\n\n"
                f"**Contract:** `{contract_address}`\n"
                f"**Name:** {contract_info['name']}\n"
                f"**Project:** {contract_info['project']}\n"
                f"**Description:** {contract_info['description']}\n\n"
                f"**Block:** {block_number}\n"
                f"**Time:** {time_str}\n"
                f"**Transaction:** `{tx_hash}`"
            )
            
            payload = {
                "content": alert_message,
                "username": "Adaptor Guardian Monitor"
            }
            
            response = requests.post(self.discord_webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            
            logger.info(f"Sent Discord alert for {event_name} event on {contract_address}")
            
        except Exception as e:
            logger.error(f"Failed to send Discord alert: {e}")
    
    def run_once(self) -> List[Dict[str, Any]]:
        """Run guardian monitoring once and return detected events"""
        logger.info("Checking for guardian and pause events...")
        
        events = self.check_guardian_events()
        
        if events:
            logger.info(f"Found {len(events)} guardian/pause events")
            for event in events:
                logger.info(f"Event: {event['event_name']} on {event['contract_address']} at block {event['block_number']}")
                self.send_discord_alert(event)
        else:
            logger.debug("No guardian/pause events detected")
        
        return events
    
    def run_continuous(self, interval_minutes: int = 5):
        """Run continuous monitoring with specified interval"""
        interval_seconds = interval_minutes * 60
        
        logger.info(f"Starting continuous guardian monitoring (interval: {interval_minutes}m)")
        
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
    """Main entry point for the adaptor guardian monitor"""
    parser = argparse.ArgumentParser(description='Monitor GuardedLiquityV2OracleAdaptor contracts for guardian and pause events')
    parser.add_argument('--once', action='store_true', help='Run once instead of continuously')
    parser.add_argument('--no-discord', action='store_true', help='Disable Discord alerts')
    parser.add_argument('--interval', type=int, default=5, help='Monitoring interval in minutes (default: 5)')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose debug logging')
    
    args = parser.parse_args()
    
    # setup logging based on verbose flag
    setup_logging(verbose=args.verbose)
    
    try:
        # initialize monitor
        monitor = AdaptorGuardianMonitor(
            disable_discord=args.no_discord
        )
        
        if args.once:
            # run once
            events = monitor.run_once()
            if events:
                print(f"\nDetected Events:")
                for event in events:
                    print(f"  {event['event_name']} on {event['contract_address']} at block {event['block_number']}")
        else:
            # run continuously
            monitor.run_continuous(interval_minutes=args.interval)
            
    except Exception as e:
        logger.error(f"Monitor failed: {e}")
        exit(1)

if __name__ == "__main__":
    main() 