#!/usr/bin/env python3
"""
Bridge Watcher - Enhanced Unified Bridge Data Monitor

This script orchestrates all bridge monitoring and validation components:
1. checkpoint_scribe - collect bridge validator sets and checkpoints from Layer
2. valset_watcher - monitor validator set updates in evm data bridge contract
3. attest_watcher - monitor attestations in evm data bridge contract
4. valset_verifier - validate valset updates in bridge contract against Layer
5. attest_verifier - validate attestations in bridge contract against Layer

Enhanced with database lock conflict handling and graceful degradation.

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
import config as config_module
from config import get_config_manager
from database_cli import DATABASE_COMMANDS
from database_manager import DatabaseLockError

class BridgeWatcher:
    def __init__(self, min_height: Optional[int] = None, disable_discord: bool = False, send_initial_ping: bool = False):
        self.running = False
        self.min_height = min_height
        self.disable_discord = disable_discord
        self.send_initial_ping = send_initial_ping
        
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
        
        # initialize components using config system (get current config dynamically)
        self.checkpoint_scribe = CheckpointScribe(
            layer_rpc_url=config_module.config.get_layer_rpc_url(),
            chain_id=config_module.config.get_chain_id(),
            output_prefix='checkpoints'
        )
        
        self.valset_watcher = ValsetWatcher(
            provider_url=config_module.config.get_evm_rpc_url(),
            bridge_address=config_module.config.get_bridge_address(),
            output_prefix='valset_updates',
            min_height=min_height
        )
        
        self.attest_watcher = AttestWatcher(
            provider_url=config_module.config.get_evm_rpc_url(),
            bridge_address=config_module.config.get_bridge_address(),
            output_prefix='attestations',
            min_height=min_height
        )
        
        self.valset_verifier = ValsetVerifier(
            layer_rpc_url=config_module.config.get_layer_rpc_url(),
            evm_rpc_url=config_module.config.get_evm_rpc_url(),
            chain_id=config_module.config.get_chain_id(),
            disable_discord=disable_discord
        )
        
        self.attest_verifier = AttestVerifier(
            layer_rpc_url=config_module.config.get_layer_rpc_url(),
            chain_id=config_module.config.get_chain_id(),
            disable_discord=disable_discord
        )
        
        # setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info("Bridge Watcher initialized")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
    
    def run_component_with_lock_handling(self, component_name: str, component_func, *args, **kwargs):
        """Run a component with database lock error handling"""
        max_retries = 3
        base_delay = 5.0
        
        for attempt in range(max_retries):
            try:
                result = component_func(*args, **kwargs)
                return result, True  # success
                
            except DatabaseLockError as e:
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"{component_name}: Database locked, retrying in {delay}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(delay)
                    continue
                else:
                    logger.error(f"{component_name}: Failed after {max_retries} attempts due to database locks")
                    return None, False  # failed after all retries
                    
            except Exception as e:
                logger.error(f"{component_name}: Unexpected error: {e}")
                return None, False  # failed with other error
        
        return None, False  # shouldn't reach here
    
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
        Run one complete monitoring cycle with lock handling
        Returns True if successful, False if error
        """
        cycle_start = datetime.utcnow()
        logger.info("=" * 60)
        logger.info(f"Starting enhanced bridge monitoring cycle at {cycle_start.isoformat()}")
        logger.info("=" * 60)
        
        cycle_success = True
        component_results = {}
        
        # Step 1: Collect Layer checkpoints (foundation data)
        logger.info("🔍 Step 1/5: Collecting Layer checkpoints...")
        checkpoint_state, success = self.run_component_with_lock_handling(
            "CheckpointScribe", 
            self.checkpoint_scribe.run_monitoring_cycle
        )
        
        if success:
            checkpoint_count = checkpoint_state.get('total_checkpoints_found', 0) if checkpoint_state else 0
            logger.info(f"✅ Checkpoint collection complete. Total: {checkpoint_count}")
            component_results['checkpoints'] = checkpoint_count
        else:
            logger.warning("⚠️  Checkpoint collection failed, continuing with other components")
            cycle_success = False
            component_results['checkpoints'] = 'failed'
        
        # Step 2: Monitor validator set updates
        logger.info("🔍 Step 2/5: Monitoring validator set updates...")
        valset_state, success = self.run_component_with_lock_handling(
            "ValsetWatcher",
            self.valset_watcher.run_monitoring_cycle
        )
        
        if success:
            valset_count = valset_state.get('total_events_found', 0) if valset_state else 0
            logger.info(f"✅ Valset monitoring complete. Total: {valset_count}")
            component_results['valset_updates'] = valset_count
        else:
            logger.warning("⚠️  Valset monitoring failed, continuing with other components")
            cycle_success = False
            component_results['valset_updates'] = 'failed'
        
        # Step 3: Monitor attestations  
        logger.info("🔍 Step 3/5: Monitoring attestations...")
        attestation_state, success = self.run_component_with_lock_handling(
            "AttestWatcher",
            self.attest_watcher.run_monitoring_cycle
        )
        
        if success:
            attestation_count = attestation_state.get('total_calls_found', 0) if attestation_state else 0
            logger.info(f"✅ Attestation monitoring complete. Total: {attestation_count}")
            component_results['attestations'] = attestation_count
        else:
            logger.warning("⚠️  Attestation monitoring failed, continuing with other components")
            cycle_success = False
            component_results['attestations'] = 'failed'
        
        # Step 4: Validate validator set updates
        logger.info("🔍 Step 4/5: Validating validator set updates...")
        valset_validation_state, success = self.run_component_with_lock_handling(
            "ValsetVerifier",
            self.valset_verifier.validate_all_valset_updates
        )
        
        if success:
            valset_validation_count = valset_validation_state.get('total_validations', 0) if valset_validation_state else 0
            logger.info(f"✅ Valset validation complete. Total: {valset_validation_count}")
            component_results['valset_validation'] = valset_validation_count
        else:
            logger.warning("⚠️  Valset validation failed, continuing with other components")
            cycle_success = False
            component_results['valset_validation'] = 'failed'
        
        # Step 5: Validate attestations
        logger.info("🔍 Step 5/5: Validating attestations...")
        attestation_validation_state, success = self.run_component_with_lock_handling(
            "AttestVerifier",
            self.attest_verifier.validate_all_attestations
        )
        
        if success:
            attestation_validation_count = attestation_validation_state.get('total_validations', 0) if attestation_validation_state else 0
            logger.info(f"✅ Attestation validation complete. Total: {attestation_validation_count}")
            component_results['attestation_validation'] = attestation_validation_count
        else:
            logger.warning("⚠️  Attestation validation failed")
            cycle_success = False
            component_results['attestation_validation'] = 'failed'
        
        cycle_end = datetime.utcnow()
        cycle_duration = (cycle_end - cycle_start).total_seconds()
        
        logger.info("=" * 60)
        if cycle_success:
            logger.info(f"✅ Bridge monitoring cycle completed successfully in {cycle_duration:.1f}s")
        else:
            logger.warning(f"⚠️  Bridge monitoring cycle completed with some failures in {cycle_duration:.1f}s")
        
        logger.info(f"📊 Component Results: {component_results}")
        logger.info("=" * 60)
        
        return cycle_success
    
    def run_continuous(self, interval_seconds: int = 300):
        """Run continuous monitoring with specified interval"""
        logger.info(f"🚀 Starting continuous bridge monitoring (interval: {interval_seconds}s)")
        
        # show configuration information
        try:
            config_manager = get_config_manager()
            logger.info(f"📋 Active Config: {config_manager.get_active_config_name()}")
        except RuntimeError:
            logger.info("📋 Active Config: Legacy Mode")
            
        logger.info(f"📍 Layer RPC: {config_module.config.get_layer_rpc_url()}")
        logger.info(f"📍 EVM RPC: {config_module.config.get_evm_rpc_url()}")
        logger.info(f"📍 Bridge Address: {config_module.config.get_bridge_address()}")
        logger.info(f"📍 Chain ID: {config_module.config.get_chain_id()}")
        
        if self.min_height is not None:
            logger.info(f"⬆️  Min Height: {self.min_height} (will override saved state if higher)")
        else:
            logger.info("⬆️  Min Height: Not set (using saved state or 21 days ago)")
        
        self.running = True
        state = self.load_watcher_state()
        
        # initial pings if requested or first-time schedule
        try:
            if hasattr(self.valset_verifier, 'ping_helper'):
                if self.send_initial_ping or self.valset_verifier.ping_helper.should_send_ping(self.valset_verifier.ping_frequency_days):
                    content = self.valset_verifier.generate_ping_content()
                    self.valset_verifier.ping_helper.send_ping(content, self.valset_verifier.ping_frequency_days, force=self.send_initial_ping)
            if hasattr(self.attest_verifier, 'ping_helper'):
                if self.send_initial_ping or self.attest_verifier.ping_helper.should_send_ping(self.attest_verifier.ping_frequency_days):
                    content = self.attest_verifier.generate_ping_content()
                    self.attest_verifier.ping_helper.send_ping(content, self.attest_verifier.ping_frequency_days, force=self.send_initial_ping)
        except Exception as e:
            logger.warning(f"Initial ping failed: {e}")

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
        
        # send initial ping if requested and supported
        try:
            if hasattr(self.valset_verifier, 'ping_helper'):
                if self.send_initial_ping or self.valset_verifier.ping_helper.should_send_ping(self.valset_verifier.ping_frequency_days):
                    content = self.valset_verifier.generate_ping_content()
                    self.valset_verifier.ping_helper.send_ping(content, self.valset_verifier.ping_frequency_days, force=self.send_initial_ping)
            if hasattr(self.attest_verifier, 'ping_helper'):
                if self.send_initial_ping or self.attest_verifier.ping_helper.should_send_ping(self.attest_verifier.ping_frequency_days):
                    content = self.attest_verifier.generate_ping_content()
                    self.attest_verifier.ping_helper.send_ping(content, self.attest_verifier.ping_frequency_days, force=self.send_initial_ping)
        except Exception as e:
            logger.warning(f"Initial ping failed: {e}")

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
            
            chain_id = config_module.config.get_chain_id()
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
        
        chain_id = config_module.config.get_chain_id()
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
        'layer_rpc_url': config_module.config.get_layer_rpc_url(),
        'evm_rpc_url': config_module.config.get_evm_rpc_url(),
        'bridge_address': config_module.config.get_bridge_address(),
        'chain_id': config_module.config.get_chain_id()
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
  bridgewatch --config layertest-4-sepolia-bridge start  # use specific config
  bridgewatch status --no-color      # show status without colors
  bridgewatch reset                  # reset all progress
  bridgewatch test-discord           # test Discord webhook alerts
  bridgewatch config list            # list all available configurations
  bridgewatch config show            # show active configuration details
  bridgewatch config switch <name>   # switch to different configuration
  bridgewatch config validate        # validate current configuration
  bridgewatch valset-alerts          # monitor and alert on validator set updates
  bridgewatch valset-alerts --config tellor-1-sagaevm-bridge  # run with specific config
        """
    )
    
    # add global flags
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    parser.add_argument('--no-color', action='store_true', help='Disable colored output')
    parser.add_argument('--config', type=str, help='Specify which configuration profile to use (overrides ACTIVE_CONFIG)')
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # start command
    start_parser = subparsers.add_parser('start', help='Start bridge monitoring')
    start_parser.add_argument('--once', action='store_true', help='Run once instead of continuously')
    start_parser.add_argument('--interval', type=int, default=300, help='Monitoring interval in seconds (default: 300)')
    start_parser.add_argument('--min-height', type=int, help='Minimum block height to start scraping from (EVM chains)')
    start_parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    start_parser.add_argument('--no-color', action='store_true', help='Disable colored output')
    start_parser.add_argument('--no-discord', action='store_true', help='Disable Discord alerts')
    start_parser.add_argument('--config', type=str, help='Specify which configuration profile to use (overrides ACTIVE_CONFIG)')
    start_parser.add_argument('--ping-now', action='store_true', help='Send immediate pings for verifiers on start')
    
    # status command
    status_parser = subparsers.add_parser('status', help='Show current status')
    status_parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    status_parser.add_argument('--no-color', action='store_true', help='Disable colored output')
    status_parser.add_argument('--config', type=str, help='Specify which configuration profile to use (overrides ACTIVE_CONFIG)')
    
    # reset command  
    reset_parser = subparsers.add_parser('reset', help='Reset all state files')
    reset_parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    reset_parser.add_argument('--no-color', action='store_true', help='Disable colored output')
    reset_parser.add_argument('--config', type=str, help='Specify which configuration profile to use (overrides ACTIVE_CONFIG)')
    
    # test-discord command
    discord_parser = subparsers.add_parser('test-discord', help='Test Discord webhook')
    discord_parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    discord_parser.add_argument('--no-color', action='store_true', help='Disable colored output')
    discord_parser.add_argument('--config', type=str, help='Specify which configuration profile to use (overrides ACTIVE_CONFIG)')
    
    # config command with subcommands
    config_parser = subparsers.add_parser('config', help='Configuration management')
    config_parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    config_parser.add_argument('--no-color', action='store_true', help='Disable colored output')
    config_parser.add_argument('--config', type=str, help='Specify which configuration profile to use (overrides ACTIVE_CONFIG)')
    
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
    
    # database command with subcommands
    database_parser = subparsers.add_parser('database', help='Database management')
    database_parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    database_parser.add_argument('--no-color', action='store_true', help='Disable colored output')
    database_parser.add_argument('--config', type=str, help='Specify which configuration profile to use (overrides ACTIVE_CONFIG)')
    
    database_subparsers = database_parser.add_subparsers(dest='database_command', help='Database commands')
    
    # add database subcommands dynamically
    for cmd_name, cmd_info in DATABASE_COMMANDS.items():
        db_cmd_parser = database_subparsers.add_parser(cmd_name, help=cmd_info['help'])
        for arg_config in cmd_info['args']:
            arg_names, arg_kwargs = arg_config
            db_cmd_parser.add_argument(*arg_names, **arg_kwargs)
    
    # valset-alerts command  
    valset_alerts_parser = subparsers.add_parser('valset-alerts', help='Monitor and alert on validator set updates')
    valset_alerts_parser.add_argument('--once', action='store_true', help='Run once instead of continuously')
    valset_alerts_parser.add_argument('--interval', type=int, default=60, help='Check interval in seconds (default: 60)')
    valset_alerts_parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    valset_alerts_parser.add_argument('--no-color', action='store_true', help='Disable colored output')
    valset_alerts_parser.add_argument('--no-discord', action='store_true', help='Disable Discord alerts')
    valset_alerts_parser.add_argument('--config', type=str, help='Specify which configuration profile to use (overrides ACTIVE_CONFIG)')
    valset_alerts_parser.add_argument('--ping-frequency', type=int, default=7, help='Ping frequency in days (default: 7)')
    valset_alerts_parser.add_argument('--ping-now', action='store_true', help='Send an immediate ping on start')
    
    # valset-stale-alerts command
    valset_stale_alerts_parser = subparsers.add_parser('valset-stale-alerts', help='Monitor and alert when validator set becomes stale')
    valset_stale_alerts_parser.add_argument('--once', action='store_true', help='Run once instead of continuously')
    valset_stale_alerts_parser.add_argument('--interval', type=int, default=300, help='Check interval in seconds (default: 300)')
    valset_stale_alerts_parser.add_argument('--stale-threshold', type=int, default=336, help='Hours after which validator set is considered stale (default: 336 = 2 weeks)')
    valset_stale_alerts_parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    valset_stale_alerts_parser.add_argument('--no-color', action='store_true', help='Disable colored output')
    valset_stale_alerts_parser.add_argument('--no-discord', action='store_true', help='Disable Discord alerts')
    valset_stale_alerts_parser.add_argument('--config', type=str, help='Specify which configuration profile to use (overrides ACTIVE_CONFIG)')
    
    args = parser.parse_args()
    
    if args.command is None:
        parser.print_help()
        sys.exit(1)
    
    # set config override if --config flag was provided
    if hasattr(args, 'config') and args.config:
        from config import set_global_config_override
        set_global_config_override(args.config)
    
    # setup colored logging based on verbose and no-color flags
    setup_logging(verbose=args.verbose, no_color=getattr(args, 'no_color', False))
    
    # log which config we're using if override was provided
    if hasattr(args, 'config') and args.config:
        logger.info(f"🔧 Using configuration: {args.config}")
    
    # validate configuration (this will raise an error if EVM_RPC_URL is missing)
    try:
        config_module.config.get_evm_rpc_url()  # This will check if the required env var is set
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    
    try:
        if args.command == 'start':
            # log warning if Discord is disabled
            if getattr(args, 'no_discord', False):
                logger.warning("⚠️  Discord alerts are DISABLED via --no-discord flag")
            
            watcher = BridgeWatcher(min_height=args.min_height, disable_discord=getattr(args, 'no_discord', False), send_initial_ping=getattr(args, 'ping_now', False))
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
        
        elif args.command == 'database':
            # handle database commands
            if not hasattr(args, 'database_command') or args.database_command is None:
                logger.error("❌ No database subcommand specified")
                logger.info("💡 Use 'bridgewatch database --help' for available commands")
                sys.exit(1)
            
            db_command = args.database_command
            if db_command in DATABASE_COMMANDS:
                DATABASE_COMMANDS[db_command]['func'](args)
            else:
                logger.error(f"❌ Unknown database command: {db_command}")
                sys.exit(1)
            
        elif args.command == 'valset-alerts':
            from valset_alerter import ValsetAlerter
            
            # log warning if Discord is disabled
            if getattr(args, 'no_discord', False):
                logger.warning("⚠️  Discord alerts are DISABLED via --no-discord flag")
            
            logger.info("🔔 Starting Valset Alerter...")
            
            try:
                alerter = ValsetAlerter(disable_discord=getattr(args, 'no_discord', False), ping_frequency_days=getattr(args, 'ping_frequency', 7))
                
                if args.once:
                    alerts_sent = alerter.run_once(send_ping=getattr(args, 'ping_now', False))
                    logger.info(f"✅ Sent {alerts_sent} valset update alerts")
                else:
                    alerter.run_continuous(args.interval, send_initial_ping=getattr(args, 'ping_now', False))
                    
            except Exception as e:
                logger.error(f"❌ Valset alerter failed: {e}")
                sys.exit(1)
        
        elif args.command == 'valset-stale-alerts':
            from valset_stale_alerter import ValsetStaleAlerter
            
            # log warning if Discord is disabled
            if getattr(args, 'no_discord', False):
                logger.warning("⚠️  Discord alerts are DISABLED via --no-discord flag")
            
            logger.info("⏰ Starting Valset Stale Alerter...")
            
            try:
                alerter = ValsetStaleAlerter(
                    disable_discord=getattr(args, 'no_discord', False),
                    stale_threshold_hours=getattr(args, 'stale_threshold', 336)
                )
                
                if args.once:
                    result = alerter.run_once()
                    if result == 1:
                        logger.warning("🚨 Validator set is STALE")
                    elif result == 0:
                        logger.info("✅ Validator set is fresh")
                    else:
                        logger.error("❌ Error checking validator set staleness")
                        sys.exit(1)
                else:
                    alerter.run_continuous(args.interval)
                    
            except Exception as e:
                logger.error(f"❌ Valset stale alerter failed: {e}")
                sys.exit(1)
            
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 