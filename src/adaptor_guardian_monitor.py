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
from ping_helper import PingHelper

# logging will be configured in main() based on --verbose flag
logger = logging.getLogger(__name__)

class AdaptorGuardianMonitor:
    def __init__(self, disable_discord: bool = False, ping_frequency_days: int = 7):
        """Initialize the adaptor guardian monitor
        
        Args:
            disable_discord: If True, skip sending Discord alerts
            ping_frequency_days: Ping frequency in days (7=weekly, 1=daily, etc.)
        """
        try:
            self.config_manager = get_config_manager()
            
            # get configuration
            self.evm_rpc_url = self.config_manager.get_evm_rpc_url()
            self.layer_chain = self.config_manager.get_layer_chain()
            self.evm_chain = self.config_manager.get_evm_chain()
            
            # store settings
            self.disable_discord = disable_discord
            self.ping_frequency_days = ping_frequency_days
            
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
            
            # initialize ping helper
            self.ping_helper = PingHelper(
                script_name="adaptor_guardian_monitor",
                data_dir=data_dir,
                discord_webhook_url=self.discord_webhook_url
            )
            
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
            webhook_url = self.config_manager.get_discord_webhook('adaptor_guardian')
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
            events = []
            
            # calculate event topic signatures
            guardian_added_topic = Web3.keccak(text="GuardianAdded(address)").hex()
            guardian_removed_topic = Web3.keccak(text="GuardianRemoved(address)").hex()
            admin_removed_topic = Web3.keccak(text="AdminRemoved()").hex()
            paused_topic = Web3.keccak(text="Paused()").hex()
            unpaused_topic = Web3.keccak(text="Unpaused()").hex()
            
            # event mappings
            event_topics = {
                guardian_added_topic: 'GuardianAdded',
                guardian_removed_topic: 'GuardianRemoved',
                admin_removed_topic: 'AdminRemoved',
                paused_topic: 'Paused',
                unpaused_topic: 'Unpaused'
            }
            
            # get all logs for all event types at once
            try:
                logs = self.w3.eth.get_logs({
                    "fromBlock": from_block,
                    "toBlock": to_block,
                    "address": contract_address,
                    "topics": [list(event_topics.keys())]
                })
                
                for log in logs:
                    topic = log['topics'][0].hex()
                    event_name = event_topics.get(topic, 'Unknown')
                    
                    # decode event args
                    args = {}
                    if event_name in ['GuardianAdded', 'GuardianRemoved'] and len(log['topics']) > 1:
                        # guardian address is the second topic (indexed parameter)
                        guardian_hex = log['topics'][1].hex()
                        # convert to address format (remove leading zeros and add 0x)
                        args['guardian'] = '0x' + guardian_hex[-40:]
                    
                    event_data = {
                        'contract_address': contract_address,
                        'event_name': event_name,
                        'block_number': log['blockNumber'],
                        'transaction_hash': log['transactionHash'].hex(),
                        'args': args
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
                logger.warning(f"Failed to get events for {contract_address}: {e}")
                return []
            
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
    
    def generate_ping_content(self) -> str:
        """Generate ping content with current admin and guardian information"""
        try:
            ping_content = (
                f"**Monitoring Guardian Events:**\n"
                f"**Adaptors Monitored:** {len(self.adaptor_contracts)}\n\n"
            )
            
            # collect admin and guardian information from all contracts
            admin_contracts = {}  # admin_address -> {project -> [descriptions]}
            guardian_contracts = {}  # guardian_address -> {project -> [descriptions]}
            
            for adaptor_config in self.adaptor_contracts:
                contract_address = adaptor_config['address']
                description = adaptor_config['description']
                
                try:
                    contract = self.contract_instances[contract_address]
                    
                    # get contract info
                    contract_info = self._get_contract_info(contract)
                    project = contract_info.get('project', 'Unknown Project')
                    
                    # get admin
                    try:
                        admin_address = contract.functions.admin().call()
                        if admin_address and admin_address != "0x0000000000000000000000000000000000000000":
                            if admin_address not in admin_contracts:
                                admin_contracts[admin_address] = {}
                            if project not in admin_contracts[admin_address]:
                                admin_contracts[admin_address][project] = []
                            admin_contracts[admin_address][project].append(description)
                    except Exception as e:
                        logger.debug(f"Could not get admin for {contract_address}: {e}")
                    
                    # get guardians
                    try:
                        guardian_count = contract.functions.guardianCount().call()
                        for i in range(guardian_count):
                            # unfortunately, we can't easily iterate guardians without knowing addresses
                            # we'll need to check known admin addresses as they're often guardians too
                            pass
                    except Exception as e:
                        logger.debug(f"Could not get guardians for {contract_address}: {e}")
                        
                except Exception as e:
                    logger.debug(f"Error processing contract {contract_address}: {e}")
            
            # add admin information
            if admin_contracts:
                ping_content += "**Admin(s):**\n"
                for admin_address, projects in admin_contracts.items():
                    ping_content += f"{admin_address}...\n"
                    for project, descriptions in projects.items():
                        descriptions_str = ", ".join(descriptions)
                        ping_content += f"{project}: {descriptions_str}\n"
                    ping_content += "\n"
            else:
                ping_content += "**Admin(s):** None found\n\n"
            
            return ping_content
            
        except Exception as e:
            logger.error(f"Failed to generate ping content: {e}")
            return f"**Status:** Error generating ping content: {e}"

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
    
    def run_once(self, send_ping: bool = False) -> List[Dict[str, Any]]:
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
        
        # check for scheduled ping
        if self.ping_helper.should_send_ping(self.ping_frequency_days) or send_ping:
            ping_content = self.generate_ping_content()
            self.ping_helper.send_ping(ping_content, self.ping_frequency_days, force=send_ping)
        
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
        monitor = AdaptorGuardianMonitor(
            disable_discord=args.no_discord,
            ping_frequency_days=args.ping_frequency
        )
        
        if args.once:
            # run once
            events = monitor.run_once(send_ping=args.ping_now)
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