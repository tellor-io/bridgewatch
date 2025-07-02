#!/usr/bin/env python3
"""
Bridge Watcher - Unified Bridge Data Monitor

This script orchestrates all bridge monitoring and validation components:
1. checkpoint_scribe - collect bridge validator sets and checkpoints from Layer
2. valset_watcher - monitor validator set updates in evm data bridge contract
3. attest_watcher - monitor attestations in evm data bridge contract
4. valset_verifier - validate valset updates in bridge contract against Layer
5. attest_verifier - validate attestations in bridge contract against Layer

Usage:
    bridgewatch start [--once] [--interval 300]
    bridgewatch status
    bridgewatch reset
"""

import argparse
import time
import sys
import os
import signal
import json
from datetime import datetime
from typing import Dict, Any, Optional
from logger_utils import setup_logging

# initial setup with INFO level
logger = setup_logging()

# import all the component modules
from checkpoint_scribe import CheckpointScribe
from valset_watcher import ValsetWatcher  
from attest_watcher import AttestWatcher
from valset_verifier import ValsetVerifier
from attest_verifier import AttestVerifier
from config import config, get_config_manager

class BridgeWatcher:
    def __init__(self, min_height: Optional[int] = None):
        self.running = False
        self.min_height = min_height
        
        # get config manager for directory paths
        try:
            config_manager = get_config_manager()
            data_dir = config_manager.get_data_dir()
            self.state_file = f"{data_dir}/bridge_watcher_state.json"
        except RuntimeError:
            # fallback to legacy paths if in legacy mode
            data_dir = "data"
            self.state_file = "data/bridge_watcher_state.json"
        
        # create main data directory
        os.makedirs(data_dir, exist_ok=True)
        
        # initialize components using config system
        self.checkpoint_scribe = CheckpointScribe(
            layer_rpc_url=config.get_layer_rpc_url(),
            chain_id=config.get_chain_id(),
            output_prefix='checkpoints'
        )
        
        self.valset_watcher = ValsetWatcher(
            provider_url=config.get_evm_rpc_url(),
            bridge_address=config.get_bridge_address(),
            output_prefix='valset_updates',
            min_height=min_height
        )
        
        self.attest_watcher = AttestWatcher(
            provider_url=config.get_evm_rpc_url(),
            bridge_address=config.get_bridge_address(),
            output_prefix='attestations',
            min_height=min_height
        )
        
        self.valset_verifier = ValsetVerifier(
            layer_rpc_url=config.get_layer_rpc_url(),
            evm_rpc_url=config.get_evm_rpc_url(),
            chain_id=config.get_chain_id()
        )
        
        self.attest_verifier = AttestVerifier(
            layer_rpc_url=config.get_layer_rpc_url(),
            chain_id=config.get_chain_id()
        )
        
        # setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info("Bridge Watcher initialized")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
    
    def load_watcher_state(self) -> Dict[str, Any]:
        """Load overall watcher state"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load watcher state: {e}")
        
        return {
            "last_cycle_timestamp": None,
            "total_cycles": 0,
            "last_successful_cycle": None
        }
    
    def save_watcher_state(self, state: Dict[str, Any]):
        """Save overall watcher state"""
        try:
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save watcher state: {e}")
    
    def run_single_cycle(self) -> bool:
        """
        Run one complete monitoring cycle
        Returns True if successful, False if error
        """
        cycle_start = datetime.utcnow()
        logger.info("=" * 60)
        logger.info(f"Starting bridge monitoring cycle at {cycle_start.isoformat()}")
        logger.info("=" * 60)
        
        try:
            # step 1: collect Layer checkpoints (foundation data)
            logger.info("🔍 Step 1/5: Collecting Layer checkpoints...")
            checkpoint_state = self.checkpoint_scribe.run_monitoring_cycle()
            checkpoint_count = checkpoint_state.get('total_checkpoints_found', 0) if checkpoint_state else 0
            logger.info(f"✅ Checkpoint collection complete. Total: {checkpoint_count}")
            
            # step 2: monitor validator set updates
            logger.info("🔍 Step 2/5: Monitoring validator set updates...")
            valset_state = self.valset_watcher.run_monitoring_cycle()
            valset_count = valset_state.get('total_events_found', 0) if valset_state else 0
            logger.info(f"✅ Valset monitoring complete. Total: {valset_count}")
            
            # step 3: monitor attestations  
            logger.info("🔍 Step 3/5: Monitoring attestations...")
            attestation_state = self.attest_watcher.run_monitoring_cycle()
            attestation_count = attestation_state.get('total_calls_found', 0) if attestation_state else 0
            logger.info(f"✅ Attestation monitoring complete. Total: {attestation_count}")
            
            # step 4: validate validator set updates
            logger.info("🔍 Step 4/5: Validating validator set updates...")
            valset_validation_state = self.valset_verifier.validate_all_valset_updates()
            valset_validation_count = valset_validation_state.get('total_validations', 0) if valset_validation_state else 0
            logger.info(f"✅ Valset validation complete. Total: {valset_validation_count}")
            
            # step 5: validate attestations
            logger.info("🔍 Step 5/5: Validating attestations...")
            attestation_validation_state = self.attest_verifier.validate_all_attestations()
            attestation_validation_count = attestation_validation_state.get('total_validations', 0) if attestation_validation_state else 0
            logger.info(f"✅ Attestation validation complete. Total: {attestation_validation_count}")
            
            cycle_end = datetime.utcnow()
            cycle_duration = (cycle_end - cycle_start).total_seconds()
            
            logger.info("=" * 60)
            logger.info(f"✅ Bridge monitoring cycle completed successfully in {cycle_duration:.1f}s")
            logger.info("=" * 60)
            
            return True
            
        except Exception as e:
            cycle_end = datetime.utcnow()
            cycle_duration = (cycle_end - cycle_start).total_seconds()
            
            logger.error("=" * 60)
            logger.error(f"❌ Bridge monitoring cycle failed after {cycle_duration:.1f}s: {e}")
            logger.error("=" * 60)
            
            return False
    
    def run_continuous(self, interval_seconds: int = 300):
        """Run continuous monitoring with specified interval"""
        logger.info(f"🚀 Starting continuous bridge monitoring (interval: {interval_seconds}s)")
        
        # show configuration information
        try:
            config_manager = get_config_manager()
            logger.info(f"📋 Active Config: {config_manager.get_display_name()}")
            logger.info(f"📂 Data Directory: {config_manager.get_data_dir()}")
        except RuntimeError:
            logger.info("📋 Active Config: Legacy Mode")
        
        logger.info(f"📍 Layer RPC: {config.get_layer_rpc_url()}")
        logger.info(f"📍 EVM RPC: {config.get_evm_rpc_url()}")
        logger.info(f"📍 Bridge Address: {config.get_bridge_address()}")
        logger.info(f"📍 Chain ID: {config.get_chain_id()}")
        
        if self.min_height is not None:
            logger.info(f"⬆️  Min Height: {self.min_height} (will override saved state if higher)")
        else:
            logger.info("⬆️  Min Height: Not set (using saved state or 21 days ago)")
        
        self.running = True
        state = self.load_watcher_state()
        
        while self.running:
            try:
                cycle_success = self.run_single_cycle()
                
                # update state
                state["last_cycle_timestamp"] = datetime.utcnow().isoformat()
                state["total_cycles"] += 1
                
                if cycle_success:
                    state["last_successful_cycle"] = state["last_cycle_timestamp"]
                
                self.save_watcher_state(state)
                
                if not self.running:
                    break
                
                # wait for next cycle
                logger.info(f"💤 Waiting {interval_seconds}s until next cycle...")
                for _ in range(interval_seconds):
                    if not self.running:
                        break
                    time.sleep(1)
                    
            except KeyboardInterrupt:
                logger.info("👋 Monitoring stopped by user")
                break
            except Exception as e:
                logger.error(f"💥 Error in monitoring cycle: {e}")
                if self.running:
                    logger.info(f"⏳ Waiting {interval_seconds}s before retry...")
                    time.sleep(interval_seconds)
    
    def run_once(self):
        """Run monitoring once and exit"""
        logger.info("🎯 Running bridge monitoring once")
        
        success = self.run_single_cycle()
        
        if success:
            logger.info("✅ Single monitoring cycle completed successfully")
            sys.exit(0)
        else:
            logger.error("❌ Single monitoring cycle failed")
            sys.exit(1)
    
    def show_status(self):
        """Show current status of all components"""
        logger.info("📊 Bridge Watcher Status")
        logger.info("=" * 50)
        
        # load states from all components
        try:
            # get config manager for directory paths
            try:
                config_manager = get_config_manager()
                checkpoint_dir = config_manager.get_layer_checkpoints_dir()
                valset_dir = config_manager.get_valset_dir()
                oracle_dir = config_manager.get_oracle_dir()
                validation_dir = config_manager.get_validation_dir()
                print(f"📋 Active Config: {config_manager.get_display_name()}")
                print(f"📂 Data Directory: {config_manager.get_data_dir()}")
            except RuntimeError:
                # fallback to legacy paths if in legacy mode
                checkpoint_dir = "data/layer_checkpoints"
                valset_dir = "data/valset"
                oracle_dir = "data/oracle"
                validation_dir = "data/validation"
                print("📋 Active Config: Legacy Mode")
                print("📂 Data Directory: data/")
            
            watcher_state = self.load_watcher_state()
            checkpoint_state = self.checkpoint_scribe.load_state()
            valset_state = self.valset_watcher.load_state()
            attestation_state = self.attest_watcher.load_state()
            
            # get validation states if files exist
            valset_validation_state = {}
            attestation_validation_state = {}
            try:
                valset_validation_state = self.valset_verifier.load_validation_state()
                attestation_validation_state = self.attest_verifier.load_validation_state()
            except:
                pass
            
            print(f"🕐 Last Cycle: {watcher_state.get('last_successful_cycle', 'Never')}")
            print(f"🔄 Total Cycles: {watcher_state.get('total_cycles', 0)}")
            print()
            print("📈 Data Collection:")
            print(f"  • Layer Checkpoints: {checkpoint_state.get('total_checkpoints_found', 0) if checkpoint_state else 0}")
            print(f"  • Valset Updates: {valset_state.get('total_events_found', 0) if valset_state else 0}")
            print(f"  • Attestations: {attestation_state.get('total_calls_found', 0) if attestation_state else 0}")
            print()
            print("🔍 Validation:")
            print(f"  • Valset Validations: {valset_validation_state.get('total_validations', 0) if valset_validation_state else 0}")
            print(f"  • Attestation Validations: {attestation_validation_state.get('total_validations', 0) if attestation_validation_state else 0}")
            print()
            print("📁 Data Files:")
            
            chain_id = config.get_chain_id()
            data_files = [
                f"{checkpoint_dir}/{chain_id}_checkpoints.csv",
                f"{valset_dir}/valset_updates.csv", 
                f"{oracle_dir}/attestations.csv",
                f"{validation_dir}/{chain_id}_valset_validation_results.csv",
                f"{validation_dir}/{chain_id}_attestation_validation_results.csv"
            ]
            
            for file_path in data_files:
                if os.path.exists(file_path):
                    size = os.path.getsize(file_path)
                    print(f"  ✅ {file_path} ({size:,} bytes)")
                else:
                    print(f"  ❌ {file_path} (missing)")
            
        except Exception as e:
            logger.error(f"Error getting status: {e}")
    
    def reset(self):
        """Reset all state files (keeps data files)"""
        logger.warning("🗑️  Resetting all state files...")
        
        # get config manager for directory paths
        try:
            config_manager = get_config_manager()
            checkpoint_dir = config_manager.get_layer_checkpoints_dir()
            valset_dir = config_manager.get_valset_dir()
            oracle_dir = config_manager.get_oracle_dir()
            validation_dir = config_manager.get_validation_dir()
            bridge_state_file = f"{config_manager.get_data_dir()}/bridge_watcher_state.json"
        except RuntimeError:
            # fallback to legacy paths if in legacy mode
            checkpoint_dir = "data/layer_checkpoints"
            valset_dir = "data/valset"
            oracle_dir = "data/oracle"
            validation_dir = "data/validation"
            bridge_state_file = "data/bridge_watcher_state.json"
        
        chain_id = config.get_chain_id()
        state_files = [
            bridge_state_file,
            f"{checkpoint_dir}/{chain_id}_checkpoints_state.json",
            f"{valset_dir}/valset_updates_state.json",
            f"{oracle_dir}/attestations_state.json", 
            f"{validation_dir}/{chain_id}_valset_validation_state.json",
            f"{validation_dir}/{chain_id}_attestation_validation_state.json"
        ]
        
        removed_count = 0
        for state_file in state_files:
            if os.path.exists(state_file):
                try:
                    os.remove(state_file)
                    logger.info(f"🗑️  Removed {state_file}")
                    removed_count += 1
                except Exception as e:
                    logger.error(f"❌ Failed to remove {state_file}: {e}")
        
        logger.info(f"✅ Reset complete. Removed {removed_count} state files.")
        logger.info("💡 Data files preserved. Next run will start from beginning.")

def load_config() -> Dict[str, Any]:
    """Load configuration from environment or defaults (deprecated - use config module instead)"""
    logger.warning("load_config() is deprecated, components should use config module directly")
    return {
        'layer_rpc_url': config.get_layer_rpc_url(),
        'evm_rpc_url': config.get_evm_rpc_url(),
        'bridge_address': config.get_bridge_address(),
        'chain_id': config.get_chain_id()
    }

def main():
    parser = argparse.ArgumentParser(
        description="Bridge Watcher - Unified Bridge Data Collector",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  bridgewatch start                  # start continuous monitoring  
  bridgewatch start --once           # run once and exit
  bridgewatch start --interval 600         # run every 10 minutes
  bridgewatch start --min-height 8500000   # start scraping from block 8500000
  bridgewatch start --verbose              # start with verbose colored logging
  bridgewatch --verbose start        # verbose flag works globally too
  bridgewatch status --no-color      # show status without colors
  bridgewatch reset                  # reset all progress
  bridgewatch test-discord           # test Discord webhook alerts
  bridgewatch config list            # list all available configurations
  bridgewatch config show            # show active configuration details
  bridgewatch config switch <name>   # switch to different configuration
  bridgewatch config validate        # validate current configuration
        """
    )
    
    # add global verbose flag
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    parser.add_argument('--no-color', action='store_true', help='Disable colored output')
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # start command
    start_parser = subparsers.add_parser('start', help='Start bridge monitoring')
    start_parser.add_argument('--once', action='store_true', help='Run once instead of continuously')
    start_parser.add_argument('--interval', type=int, default=300, help='Monitoring interval in seconds (default: 300)')
    start_parser.add_argument('--min-height', type=int, help='Minimum block height to start scraping from (EVM chains)')
    start_parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    start_parser.add_argument('--no-color', action='store_true', help='Disable colored output')
    
    # status command
    status_parser = subparsers.add_parser('status', help='Show current status')
    status_parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    status_parser.add_argument('--no-color', action='store_true', help='Disable colored output')
    
    # reset command  
    reset_parser = subparsers.add_parser('reset', help='Reset all state files')
    reset_parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    reset_parser.add_argument('--no-color', action='store_true', help='Disable colored output')
    
    # test-discord command
    discord_parser = subparsers.add_parser('test-discord', help='Test Discord webhook')
    discord_parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    discord_parser.add_argument('--no-color', action='store_true', help='Disable colored output')
    
    # config command with subcommands
    config_parser = subparsers.add_parser('config', help='Configuration management')
    config_parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    config_parser.add_argument('--no-color', action='store_true', help='Disable colored output')
    
    config_subparsers = config_parser.add_subparsers(dest='config_command', help='Config commands')
    
    # config list
    config_list_parser = config_subparsers.add_parser('list', help='List all available configurations')
    
    # config show
    config_show_parser = config_subparsers.add_parser('show', help='Show active configuration details')
    
    # config switch
    config_switch_parser = config_subparsers.add_parser('switch', help='Switch active configuration')
    config_switch_parser.add_argument('config_name', help='Name of configuration to switch to')
    
    # config validate
    config_validate_parser = config_subparsers.add_parser('validate', help='Validate current configuration')
    config_validate_parser.add_argument('config_name', nargs='?', help='Configuration to validate (default: active config)')
    
    args = parser.parse_args()
    
    if args.command is None:
        parser.print_help()
        sys.exit(1)
    
    # setup colored logging based on verbose and no-color flags
    setup_logging(verbose=args.verbose, no_color=getattr(args, 'no_color', False))
    
    # validate configuration (this will raise an error if EVM_RPC_URL is missing)
    try:
        config.get_evm_rpc_url()  # This will check if the required env var is set
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    
    try:
        if args.command == 'start':
            watcher = BridgeWatcher(min_height=args.min_height)
            if args.once:
                watcher.run_once()
            else:
                watcher.run_continuous(args.interval)
                
        elif args.command == 'status':
            watcher = BridgeWatcher()
            watcher.show_status()
            
        elif args.command == 'reset':
            watcher = BridgeWatcher()
            watcher.reset()
            
        elif args.command == 'test-discord':
            # test Discord webhook directly without full initialization
            logger.info("🧪 Testing Discord webhook...")
            
            discord_webhook_url = os.getenv('DISCORD_WEBHOOK_URL')
            if not discord_webhook_url:
                logger.error("❌ DISCORD_WEBHOOK_URL environment variable not set")
                logger.info("💡 Set DISCORD_WEBHOOK_URL in your .env file or environment")
                sys.exit(1)
            
            # create test data
            test_attestation = {
                'tx_hash': '0x1234567890abcdef1234567890abcdef12345678',
                'block_number': 12345
            }
            test_result = {
                'query_id': '0xabcdef1234567890abcdef1234567890abcdef12',
                'snapshot': '0x1234567890abcdef1234567890abcdef12345678',
                'signature_verification': {
                    'verified_signatures': [
                        {'address': '0x1234567890abcdef12345678', 'power': 1000},
                        {'address': '0xabcdef1234567890abcdef12', 'power': 750}
                    ],
                    'signing_percentage': 100.0,
                    'total_signing_power': 1750,
                    'total_validator_power': 4970
                }
            }
            
            # test the alert function
            try:
                watcher = BridgeWatcher()
                watcher.attest_verifier.send_discord_alert('malicious_attestation', test_attestation, test_result)
                logger.info("✅ Discord test message sent successfully!")
                logger.info("💡 Check your Discord channel for the test alert")
            except Exception as e:
                logger.error(f"❌ Discord test failed: {e}")
                sys.exit(1)
        
        elif args.command == 'config':
            # handle configuration commands
            if not hasattr(args, 'config_command') or args.config_command is None:
                logger.error("❌ No config subcommand specified")
                logger.info("💡 Use 'bridgewatch config --help' for available commands")
                sys.exit(1)
            
            try:
                config_manager = get_config_manager()
            except Exception as e:
                logger.error(f"❌ Failed to load configuration: {e}")
                sys.exit(1)
            
            if args.config_command == 'list':
                logger.info("📋 Available Configurations:")
                configs = config_manager.list_configs()
                active_config = config_manager.get_active_config_name()
                
                for name, display_name in configs.items():
                    marker = "🔸" if name == active_config else "  "
                    print(f"{marker} {name}: {display_name}")
                
                print(f"\n✅ Active: {active_config}")
                
            elif args.config_command == 'show':
                logger.info("📋 Active Configuration Details:")
                active_config = config_manager.get_active_config()
                active_name = config_manager.get_active_config_name()
                
                print(f"Name: {active_name}")
                print(f"Display Name: {active_config.get('display_name', active_name)}")
                print(f"Layer Chain: {active_config.get('layer_chain')}")
                print(f"EVM Chain: {active_config.get('evm_chain')}")
                print(f"Bridge Contract: {active_config.get('bridge_contract')}")
                print(f"Layer RPC: {active_config.get('layer_rpc_url')}")
                print(f"EVM RPC: {active_config.get('evm_rpc_url')}")
                print(f"Data Directory: {active_config.get('data_dir')}")
                
                # validate config
                validation = config_manager.validate_config()
                if validation['valid']:
                    print("✅ Configuration is valid")
                else:
                    print("❌ Configuration has issues:")
                    for error in validation['errors']:
                        print(f"  • {error}")
                    for warning in validation['warnings']:
                        print(f"  ⚠️ {warning}")
                
            elif args.config_command == 'switch':
                config_name = args.config_name
                logger.info(f"🔄 Switching to configuration: {config_name}")
                
                try:
                    config_manager.switch_config(config_name)
                    logger.info(f"✅ Successfully switched to '{config_name}'")
                    logger.info("💡 This change affects the current session only")
                    logger.info("💡 Set ACTIVE_CONFIG in your .env file to make it permanent")
                except ValueError as e:
                    logger.error(f"❌ {e}")
                    sys.exit(1)
                
            elif args.config_command == 'validate':
                config_name = args.config_name
                if config_name:
                    logger.info(f"🔍 Validating configuration: {config_name}")
                else:
                    config_name = config_manager.get_active_config_name()
                    logger.info(f"🔍 Validating active configuration: {config_name}")
                
                validation = config_manager.validate_config(config_name)
                
                if validation['valid']:
                    logger.info("✅ Configuration is valid")
                else:
                    logger.error("❌ Configuration validation failed:")
                    for error in validation['errors']:
                        print(f"  • {error}")
                    sys.exit(1)
                
                if validation['warnings']:
                    logger.warning("⚠️ Configuration warnings:")
                    for warning in validation['warnings']:
                        print(f"  • {warning}")
            
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 