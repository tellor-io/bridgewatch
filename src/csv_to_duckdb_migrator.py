"""
CSV to DuckDB Migration Tool

Migrates existing CSV files and JSON state files to DuckDB database format.
Supports migrating data for specific configurations or all configurations.
"""

import os
import csv
import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

from config_manager import ConfigManager
from database_manager import BridgeMonitorDB

logger = logging.getLogger(__name__)

class CSVToDuckDBMigrator:
    """Migrates CSV files and JSON state to DuckDB format"""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        
    def migrate_all_configs(self, dry_run: bool = False):
        """Migrate all available configurations"""
        configs = self.config_manager.get_available_configs()
        
        for config_name in configs:
            logger.info(f"Migrating configuration: {config_name}")
            try:
                self.migrate_config(config_name, dry_run=dry_run)
                logger.info(f"✅ Successfully migrated {config_name}")
            except Exception as e:
                logger.error(f"❌ Failed to migrate {config_name}: {e}")
    
    def migrate_config(self, config_name: str, dry_run: bool = False):
        """Migrate a specific configuration from CSV to DuckDB"""
        # switch to the target config
        original_config = self.config_manager.get_active_config_name()
        self.config_manager.switch_config(config_name)
        
        try:
            # create database manager
            db = self.config_manager.create_database_manager()
            
            # get data directory
            data_dir = self.config_manager.get_data_dir()
            
            if not os.path.exists(data_dir):
                logger.warning(f"Data directory not found: {data_dir}")
                return
            
            migration_stats = {
                'layer_checkpoints': 0,
                'layer_validator_sets': 0,
                'evm_valset_updates': 0,
                'evm_attestations': 0,
                'valset_validation_results': 0,
                'attestation_validation_results': 0,
                'evidence_commands': 0,
                'component_states': 0
            }
            
            # migrate layer checkpoints
            checkpoint_file = f"{data_dir}/layer_checkpoints/{self.config_manager.get_layer_chain()}_checkpoints.csv"
            migration_stats['layer_checkpoints'] = self._migrate_layer_checkpoints(db, checkpoint_file, dry_run)
            
            # migrate layer validator sets
            valset_file = f"{data_dir}/layer_checkpoints/{self.config_manager.get_layer_chain()}_validator_sets.csv"
            migration_stats['layer_validator_sets'] = self._migrate_layer_validator_sets(db, valset_file, dry_run)
            
            # migrate EVM valset updates
            evm_valset_file = f"{data_dir}/valset/valset_updates.csv"
            migration_stats['evm_valset_updates'] = self._migrate_evm_valset_updates(db, evm_valset_file, dry_run)
            
            # migrate EVM attestations
            attestations_file = f"{data_dir}/oracle/attestations.csv"
            migration_stats['evm_attestations'] = self._migrate_evm_attestations(db, attestations_file, dry_run)
            
            # migrate validation results
            chain_id = self.config_manager.get_layer_chain()
            valset_validation_file = f"{data_dir}/validation/{chain_id}_valset_validation_results.csv"
            migration_stats['valset_validation_results'] = self._migrate_valset_validation_results(db, valset_validation_file, dry_run)
            
            attestation_validation_file = f"{data_dir}/validation/{chain_id}_attestation_validation_results.csv"
            migration_stats['attestation_validation_results'] = self._migrate_attestation_validation_results(db, attestation_validation_file, dry_run)
            
            # migrate evidence commands
            evidence_file = f"{data_dir}/validation/{chain_id}_valset_evidence_commands.txt"
            migration_stats['evidence_commands'] = self._migrate_evidence_commands(db, evidence_file, dry_run)
            
            # migrate component states
            migration_stats['component_states'] = self._migrate_component_states(db, data_dir, dry_run)
            
            # print migration summary
            self._print_migration_summary(config_name, migration_stats, dry_run)
            
        finally:
            # restore original config
            if original_config != config_name:
                self.config_manager.switch_config(original_config)
    
    def _migrate_layer_checkpoints(self, db: BridgeMonitorDB, csv_file: str, dry_run: bool) -> int:
        """Migrate layer checkpoints CSV file"""
        if not os.path.exists(csv_file):
            logger.info(f"Layer checkpoints file not found: {csv_file}")
            return 0
        
        count = 0
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    data = {
                        'scrape_timestamp': self._parse_timestamp(row['scrape_timestamp']),
                        'validator_index': int(row['validator_index']),
                        'validator_timestamp': int(row['validator_timestamp']),
                        'power_threshold': int(row['power_threshold']),
                        'validator_set_hash': row['validator_set_hash'],
                        'checkpoint': row['checkpoint']
                    }
                    
                    if not dry_run:
                        db.insert_layer_checkpoint(data)
                    count += 1
                    
                except Exception as e:
                    logger.warning(f"Failed to migrate checkpoint row: {e}")
        
        return count
    
    def _migrate_layer_validator_sets(self, db: BridgeMonitorDB, csv_file: str, dry_run: bool) -> int:
        """Migrate layer validator sets CSV file"""
        if not os.path.exists(csv_file):
            logger.info(f"Layer validator sets file not found: {csv_file}")
            return 0
        
        count = 0
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    data = {
                        'scrape_timestamp': self._parse_timestamp(row['scrape_timestamp']),
                        'valset_timestamp': int(row['valset_timestamp']),
                        'valset_checkpoint': row['valset_checkpoint'],
                        'ethereum_address': row['ethereum_address'],
                        'power': int(row['power'])
                    }
                    
                    if not dry_run:
                        db.insert_layer_validator_set(data)
                    count += 1
                    
                except Exception as e:
                    logger.warning(f"Failed to migrate validator set row: {e}")
        
        return count
    
    def _migrate_evm_valset_updates(self, db: BridgeMonitorDB, csv_file: str, dry_run: bool) -> int:
        """Migrate EVM valset updates CSV file"""
        if not os.path.exists(csv_file):
            logger.info(f"EVM valset updates file not found: {csv_file}")
            return 0
        
        count = 0
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    data = {
                        'timestamp': self._parse_timestamp(row['timestamp']),
                        'block_number': int(row['block_number']),
                        'tx_hash': row['tx_hash'],
                        'from_address': row['from_address'],
                        'to_address': row['to_address'],
                        'input_data': row['input_data'],
                        'gas_used': int(row['gas_used']),
                        'trace_address': json.loads(row['trace_address']) if row['trace_address'] else []
                    }
                    
                    if not dry_run:
                        db.insert_evm_valset_update(data)
                    count += 1
                    
                except Exception as e:
                    logger.warning(f"Failed to migrate valset update row: {e}")
        
        return count
    
    def _migrate_evm_attestations(self, db: BridgeMonitorDB, csv_file: str, dry_run: bool) -> int:
        """Migrate EVM attestations CSV file"""
        if not os.path.exists(csv_file):
            logger.info(f"EVM attestations file not found: {csv_file}")
            return 0
        
        count = 0
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    data = {
                        'timestamp': self._parse_timestamp(row['timestamp']),
                        'block_number': int(row['block_number']),
                        'tx_hash': row['tx_hash'],
                        'from_address': row['from_address'],
                        'to_address': row['to_address'],
                        'input_data': row['input_data'],
                        'gas_used': int(row['gas_used']),
                        'trace_address': json.loads(row['trace_address']) if row['trace_address'] else []
                    }
                    
                    if not dry_run:
                        db.insert_evm_attestation(data)
                    count += 1
                    
                except Exception as e:
                    logger.warning(f"Failed to migrate attestation row: {e}")
        
        return count
    
    def _migrate_valset_validation_results(self, db: BridgeMonitorDB, csv_file: str, dry_run: bool) -> int:
        """Migrate valset validation results CSV file"""
        if not os.path.exists(csv_file):
            logger.info(f"Valset validation results file not found: {csv_file}")
            return 0
        
        count = 0
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    data = {
                        'timestamp': self._parse_timestamp(row['timestamp']),
                        'block_number': int(row['block_number']),
                        'tx_hash': row['tx_hash'],
                        'validation_status': row['validation_status'],
                        'error_message': row.get('error_message'),
                        'validator_count': int(row['validator_count']) if row.get('validator_count') else None,
                        'valid_signatures': int(row['valid_signatures']) if row.get('valid_signatures') else None,
                        'invalid_signatures': int(row['invalid_signatures']) if row.get('invalid_signatures') else None,
                        'layer_checkpoint': row.get('layer_checkpoint'),
                        'evm_checkpoint': row.get('evm_checkpoint'),
                        'is_malicious': row.get('is_malicious', '').lower() == 'true'
                    }
                    
                    if not dry_run:
                        db.insert_valset_validation_result(data)
                    count += 1
                    
                except Exception as e:
                    logger.warning(f"Failed to migrate valset validation row: {e}")
        
        return count
    
    def _migrate_attestation_validation_results(self, db: BridgeMonitorDB, csv_file: str, dry_run: bool) -> int:
        """Migrate attestation validation results CSV file"""
        if not os.path.exists(csv_file):
            logger.info(f"Attestation validation results file not found: {csv_file}")
            return 0
        
        count = 0
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    data = {
                        'timestamp': self._parse_timestamp(row['timestamp']),
                        'block_number': int(row['block_number']),
                        'tx_hash': row['tx_hash'],
                        'validation_status': row['validation_status'],
                        'error_message': row.get('error_message'),
                        'oracle_data_hash': row.get('oracle_data_hash'),
                        'layer_checkpoint': row.get('layer_checkpoint'),
                        'snapshot_exists': row.get('snapshot_exists', '').lower() == 'true',
                        'is_malicious': row.get('is_malicious', '').lower() == 'true'
                    }
                    
                    if not dry_run:
                        db.insert_attestation_validation_result(data)
                    count += 1
                    
                except Exception as e:
                    logger.warning(f"Failed to migrate attestation validation row: {e}")
        
        return count
    
    def _migrate_evidence_commands(self, db: BridgeMonitorDB, txt_file: str, dry_run: bool) -> int:
        """Migrate evidence commands text file"""
        if not os.path.exists(txt_file):
            logger.info(f"Evidence commands file not found: {txt_file}")
            return 0
        
        count = 0
        with open(txt_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        # try to parse evidence command format
                        # format typically: "layer tx validator slash <validator_addr> <tx_hash> ..."
                        data = {
                            'evidence_type': 'valset_slash',
                            'tx_hash': None,
                            'validator_address': None,
                            'command_text': line
                        }
                        
                        # extract tx hash and validator address if possible
                        parts = line.split()
                        for i, part in enumerate(parts):
                            if part.startswith('0x') and len(part) == 66:  # tx hash
                                data['tx_hash'] = part
                            elif part.startswith('0x') and len(part) == 42:  # address
                                data['validator_address'] = part
                        
                        if not dry_run:
                            db.insert_evidence_command(data)
                        count += 1
                        
                    except Exception as e:
                        logger.warning(f"Failed to migrate evidence command: {e}")
        
        return count
    
    def _migrate_component_states(self, db: BridgeMonitorDB, data_dir: str, dry_run: bool) -> int:
        """Migrate JSON state files to database"""
        state_files = [
            ('bridge_watcher', f"{data_dir}/bridge_watcher_state.json"),
            ('checkpoint_scribe', f"{data_dir}/layer_checkpoints/{self.config_manager.get_layer_chain()}_checkpoints_state.json"),
            ('valset_watcher', f"{data_dir}/valset/valset_updates_state.json"),
            ('attest_watcher', f"{data_dir}/oracle/attestations_state.json"),
            ('valset_verifier', f"{data_dir}/validation/{self.config_manager.get_layer_chain()}_valset_validation_state.json"),
            ('attest_verifier', f"{data_dir}/validation/{self.config_manager.get_layer_chain()}_attestation_validation_state.json")
        ]
        
        count = 0
        for component_name, state_file in state_files:
            if os.path.exists(state_file):
                try:
                    with open(state_file, 'r') as f:
                        state_data = json.load(f)
                    
                    if not dry_run:
                        # clear existing state and set new state
                        db.update_component_state(component_name, **state_data)
                    count += 1
                    
                    logger.info(f"Migrated state: {component_name}")
                    
                except Exception as e:
                    logger.warning(f"Failed to migrate state file {state_file}: {e}")
        
        return count
    
    def _parse_timestamp(self, timestamp_str: str) -> str:
        """Parse timestamp string to standard format"""
        try:
            # try parsing as ISO format
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return dt.isoformat()
        except:
            # if that fails, return as-is
            return timestamp_str
    
    def _print_migration_summary(self, config_name: str, stats: Dict[str, int], dry_run: bool):
        """Print migration summary"""
        action = "Would migrate" if dry_run else "Migrated"
        
        logger.info(f"\n📊 Migration Summary for {config_name}:")
        logger.info(f"  Layer Checkpoints: {action} {stats['layer_checkpoints']} records")
        logger.info(f"  Layer Validator Sets: {action} {stats['layer_validator_sets']} records")
        logger.info(f"  EVM Valset Updates: {action} {stats['evm_valset_updates']} records")
        logger.info(f"  EVM Attestations: {action} {stats['evm_attestations']} records")
        logger.info(f"  Valset Validation Results: {action} {stats['valset_validation_results']} records")
        logger.info(f"  Attestation Validation Results: {action} {stats['attestation_validation_results']} records")
        logger.info(f"  Evidence Commands: {action} {stats['evidence_commands']} records")
        logger.info(f"  Component States: {action} {stats['component_states']} files")
        
        total_records = sum(stats.values())
        logger.info(f"  Total: {action} {total_records} records/files")
    
    def validate_migration(self, config_name: str) -> Dict[str, Any]:
        """Validate that migration was successful by comparing record counts"""
        # switch to target config
        original_config = self.config_manager.get_active_config_name()
        self.config_manager.switch_config(config_name)
        
        try:
            db = self.config_manager.create_database_manager()
            data_dir = self.config_manager.get_data_dir()
            
            validation_results = {}
            
            # count CSV records vs database records
            csv_files = [
                ('layer_checkpoints', f"{data_dir}/layer_checkpoints/{self.config_manager.get_layer_chain()}_checkpoints.csv"),
                ('layer_validator_sets', f"{data_dir}/layer_checkpoints/{self.config_manager.get_layer_chain()}_validator_sets.csv"),
                ('evm_valset_updates', f"{data_dir}/valset/valset_updates.csv"),
                ('evm_attestations', f"{data_dir}/oracle/attestations.csv"),
                ('valset_validation_results', f"{data_dir}/validation/{self.config_manager.get_layer_chain()}_valset_validation_results.csv"),
                ('attestation_validation_results', f"{data_dir}/validation/{self.config_manager.get_layer_chain()}_attestation_validation_results.csv")
            ]
            
            stats = db.get_statistics()
            
            for table_name, csv_file in csv_files:
                if os.path.exists(csv_file):
                    with open(csv_file, 'r') as f:
                        csv_count = sum(1 for line in f) - 1  # subtract header
                else:
                    csv_count = 0
                
                db_count = stats.get(f'{table_name}_count', 0)
                
                validation_results[table_name] = {
                    'csv_count': csv_count,
                    'db_count': db_count,
                    'match': csv_count == db_count
                }
            
            return validation_results
            
        finally:
            if original_config != config_name:
                self.config_manager.switch_config(original_config) 