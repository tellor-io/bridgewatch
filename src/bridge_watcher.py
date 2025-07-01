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
from config import config

class BridgeWatcher:
    def __init__(self):
        self.running = False
        
        # create main data directory
        os.makedirs("data", exist_ok=True)
        
        # initialize components using config system
        self.checkpoint_scribe = CheckpointScribe(
            layer_rpc_url=config.get_layer_rpc_url(),
            chain_id=config.get_chain_id(),
            output_prefix='checkpoints'
        )
        
        self.valset_watcher = ValsetWatcher(
            provider_url=config.get_evm_rpc_url(),
            bridge_address=config.get_bridge_address(),
            output_prefix='valset_updates'
        )
        
        self.attest_watcher = AttestWatcher(
            provider_url=config.get_evm_rpc_url(),
            bridge_address=config.get_bridge_address(),
            output_prefix='attestations'
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
        
        # state file for the overall watcher
        self.state_file = "data/bridge_watcher_state.json"
        
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
        logger.info(f"📍 Layer RPC: {config.get_layer_rpc_url()}")
        logger.info(f"📍 EVM RPC: {config.get_evm_rpc_url()}")
        logger.info(f"📍 Bridge Address: {config.get_bridge_address()}")
        logger.info(f"📍 Chain ID: {config.get_chain_id()}")
        
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
                f"data/layer_checkpoints/{chain_id}_checkpoints.csv",
                "data/valset/valset_updates.csv", 
                "data/oracle/attestations.csv",
                f"data/validation/{chain_id}_valset_validation_results.csv",
                f"data/validation/{chain_id}_attestation_validation_results.csv"
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
        
        chain_id = config.get_chain_id()
        state_files = [
            "data/bridge_watcher_state.json",
            f"data/layer_checkpoints/{chain_id}_checkpoints_state.json",
            "data/valset/valset_updates_state.json",
            "data/oracle/attestations_state.json", 
            f"data/validation/{chain_id}_valset_validation_state.json",
            f"data/validation/{chain_id}_attestation_validation_state.json"
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
  bridgewatch start --interval 600   # run every 10 minutes
  bridgewatch start --verbose        # start with verbose colored logging
  bridgewatch --verbose start        # verbose flag works globally too
  bridgewatch status --no-color      # show status without colors
  bridgewatch reset                  # reset all progress
  bridgewatch test-discord           # test Discord webhook alerts
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
            watcher = BridgeWatcher()
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
            
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 