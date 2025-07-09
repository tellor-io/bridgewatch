"""
Database CLI Commands for Bridge Monitor

Provides database management functionality through the bridgewatch CLI:
- Initialize database schema
- Show database statistics
- Migrate from CSV to DuckDB
- Reset database state
- Backup and restore operations
"""

import logging
import sys
import os
from typing import Optional

from config_manager import ConfigManager
from config import get_config_manager
from database_manager import BridgeMonitorDB
from csv_to_duckdb_migrator import CSVToDuckDBMigrator

logger = logging.getLogger(__name__)

def cmd_database_init(args):
    """Initialize database schema for active configuration"""
    try:
        config_manager = get_config_manager()
        config_name = config_manager.get_active_config_name()
        
        print(f"🔧 Initializing database for configuration: {config_name}")
        
        db = config_manager.create_database_manager()
        
        print(f"✅ Database initialized successfully")
        print(f"📍 Database location: {config_manager.get_database_path()}")
        
        # show initial statistics
        stats = db.get_statistics()
        print(f"📊 Database is ready with {len(stats)} tables created")
        
    except Exception as e:
        print(f"❌ Failed to initialize database: {e}")
        sys.exit(1)

def cmd_database_stats(args):
    """Show database statistics for active configuration"""
    try:
        config_manager = get_config_manager()
        config_name = config_manager.get_active_config_name()
        
        print(f"📊 Database Statistics for: {config_manager.get_display_name()}")
        print(f"🔗 Configuration: {config_name}")
        print(f"📍 Database: {config_manager.get_database_path()}")
        print()
        
        db = config_manager.create_database_manager()
        stats = db.get_statistics()
        
        # table statistics
        print("📋 Table Statistics:")
        table_stats = [
            ('Layer Checkpoints', stats.get('layer_checkpoints_count', 0)),
            ('Layer Validator Sets', stats.get('layer_validator_sets_count', 0)),
            ('EVM Valset Updates', stats.get('evm_valset_updates_count', 0)),
            ('EVM Attestations', stats.get('evm_attestations_count', 0)),
            ('Valset Validation Results', stats.get('valset_validation_results_count', 0)),
            ('Attestation Validation Results', stats.get('attestation_validation_results_count', 0)),
            ('Evidence Commands', stats.get('evidence_commands_count', 0))
        ]
        
        for name, count in table_stats:
            print(f"  {name:30} {count:>10,}")
        
        total_records = sum(stat[1] for stat in table_stats)
        print(f"  {'Total Records':30} {total_records:>10,}")
        print()
        
        # latest data timestamps
        print("⏰ Latest Data:")
        if stats.get('latest_checkpoint_timestamp'):
            print(f"  Latest Layer Checkpoint: {stats['latest_checkpoint_timestamp']}")
        if stats.get('latest_valset_block'):
            print(f"  Latest Valset Block: {stats['latest_valset_block']:,}")
        if stats.get('latest_attestation_block'):
            print(f"  Latest Attestation Block: {stats['latest_attestation_block']:,}")
        
        # recent malicious activity
        malicious_activity = db.get_recent_malicious_activity(5)
        if malicious_activity:
            print()
            print("🚨 Recent Malicious Activity:")
            for activity in malicious_activity:
                print(f"  {activity['timestamp']} - {activity['type']} - {activity['tx_hash'][:12]}...")
        else:
            print()
            print("✅ No recent malicious activity detected")
        
    except Exception as e:
        print(f"❌ Failed to get database statistics: {e}")
        sys.exit(1)

def cmd_database_migrate(args):
    """Migrate CSV data to DuckDB for active or all configurations"""
    try:
        config_manager = get_config_manager()
        migrator = CSVToDuckDBMigrator(config_manager)
        
        # check if --all flag is set
        migrate_all = getattr(args, 'all', False)
        dry_run = getattr(args, 'dry_run', False)
        
        if dry_run:
            print("🔍 DRY RUN MODE - No actual changes will be made")
            print()
        
        if migrate_all:
            print("🚚 Migrating all configurations from CSV to DuckDB...")
            migrator.migrate_all_configs(dry_run=dry_run)
        else:
            config_name = config_manager.get_active_config_name()
            print(f"🚚 Migrating configuration '{config_name}' from CSV to DuckDB...")
            migrator.migrate_config(config_name, dry_run=dry_run)
        
        if not dry_run:
            print()
            print("✅ Migration completed successfully!")
            print("💡 Run 'bridgewatch database stats' to verify the migration")
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        sys.exit(1)

def cmd_database_validate(args):
    """Validate migration by comparing CSV and database record counts"""
    try:
        config_manager = get_config_manager()
        migrator = CSVToDuckDBMigrator(config_manager)
        
        validate_all = getattr(args, 'all', False)
        
        if validate_all:
            configs = config_manager.get_available_configs()
            for config_name in configs:
                print(f"🔍 Validating migration for: {config_name}")
                results = migrator.validate_migration(config_name)
                _print_validation_results(config_name, results)
                print()
        else:
            config_name = config_manager.get_active_config_name()
            print(f"🔍 Validating migration for: {config_name}")
            results = migrator.validate_migration(config_name)
            _print_validation_results(config_name, results)
        
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        sys.exit(1)

def _print_validation_results(config_name: str, results: dict):
    """Print validation results in a formatted table"""
    print(f"📊 Validation Results for {config_name}:")
    print(f"{'Table':<30} {'CSV Count':<12} {'DB Count':<12} {'Status':<10}")
    print("-" * 70)
    
    all_match = True
    for table_name, result in results.items():
        csv_count = result['csv_count']
        db_count = result['db_count']
        status = "✅ MATCH" if result['match'] else "❌ MISMATCH"
        
        if not result['match']:
            all_match = False
        
        print(f"{table_name:<30} {csv_count:<12,} {db_count:<12,} {status:<10}")
    
    print("-" * 70)
    if all_match:
        print("✅ All tables match - migration is valid")
    else:
        print("❌ Some tables don't match - check migration")

def cmd_database_reset(args):
    """Reset database state (keeps data, clears component states)"""
    try:
        config_manager = get_config_manager()
        config_name = config_manager.get_active_config_name()
        
        print(f"🗑️ Resetting database state for: {config_name}")
        print("⚠️  This will clear all component state data but keep monitoring data")
        
        # confirm reset
        if not getattr(args, 'force', False):
            response = input("Are you sure you want to reset? (y/N): ")
            if response.lower() != 'y':
                print("Reset cancelled")
                return
        
        db = config_manager.create_database_manager()
        
        # clear all component states
        components = ['bridge_watcher', 'checkpoint_scribe', 'valset_watcher', 
                     'attest_watcher', 'valset_verifier', 'attest_verifier']
        
        for component in components:
            db.update_component_state(component)  # empty state
        
        print("✅ Database state reset successfully")
        print("💡 Next monitoring run will start from the beginning")
        
    except Exception as e:
        print(f"❌ Failed to reset database: {e}")
        sys.exit(1)

def cmd_database_backup(args):
    """Create a backup of the database"""
    try:
        config_manager = get_config_manager()
        config_name = config_manager.get_active_config_name()
        
        # generate backup filename
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = getattr(args, 'name', f"{config_name}_{timestamp}")
        backup_dir = "backups"
        backup_path = f"{backup_dir}/{backup_name}"
        
        os.makedirs(backup_dir, exist_ok=True)
        
        print(f"💾 Creating backup for configuration: {config_name}")
        print(f"📁 Backup location: {backup_path}")
        
        db = config_manager.create_database_manager()
        db.backup_database(backup_path)
        
        print(f"✅ Backup created successfully")
        
    except Exception as e:
        print(f"❌ Failed to create backup: {e}")
        sys.exit(1)

def cmd_database_vacuum(args):
    """Optimize database by reclaiming space"""
    try:
        config_manager = get_config_manager()
        config_name = config_manager.get_active_config_name()
        
        print(f"🧹 Optimizing database for: {config_name}")
        
        db = config_manager.create_database_manager()
        db.vacuum_database()
        
        print("✅ Database optimized successfully")
        
    except Exception as e:
        print(f"❌ Failed to optimize database: {e}")
        sys.exit(1)

def cmd_database_query(args):
    """Execute a custom SQL query against the database"""
    try:
        config_manager = get_config_manager()
        config_name = config_manager.get_active_config_name()
        
        query = getattr(args, 'query', None)
        if not query:
            print("❌ No query provided. Use --query 'SELECT ...'")
            sys.exit(1)
        
        print(f"🔍 Executing query on {config_name}:")
        print(f"   {query}")
        print()
        
        db = config_manager.create_database_manager()
        
        with db.get_connection() as conn:
            try:
                results = conn.execute(query).fetchall()
                
                if results:
                    # print results in a simple table format
                    if len(results) > 0:
                        # get column names
                        columns = [desc[0] for desc in conn.description] if conn.description else []
                        
                        if columns:
                            print(" | ".join(columns))
                            print("-" * (sum(len(col) for col in columns) + len(columns) * 3 - 3))
                        
                        for row in results:
                            print(" | ".join(str(val) for val in row))
                        
                        print()
                        print(f"📊 {len(results)} rows returned")
                else:
                    print("📄 Query executed successfully (no results)")
                    
            except Exception as e:
                print(f"❌ Query failed: {e}")
                sys.exit(1)
        
    except Exception as e:
        print(f"❌ Failed to execute query: {e}")
        sys.exit(1)

# database command registry
DATABASE_COMMANDS = {
    'init': {
        'func': cmd_database_init,
        'help': 'Initialize database schema',
        'args': []
    },
    'stats': {
        'func': cmd_database_stats,
        'help': 'Show database statistics', 
        'args': []
    },
    'migrate': {
        'func': cmd_database_migrate,
        'help': 'Migrate CSV data to DuckDB',
        'args': [
            (['--all'], {'action': 'store_true', 'help': 'Migrate all configurations'}),
            (['--dry-run'], {'action': 'store_true', 'help': 'Show what would be migrated without changes'})
        ]
    },
    'validate': {
        'func': cmd_database_validate,
        'help': 'Validate migration by comparing record counts',
        'args': [
            (['--all'], {'action': 'store_true', 'help': 'Validate all configurations'})
        ]
    },
    'reset': {
        'func': cmd_database_reset,
        'help': 'Reset database state (keeps data)',
        'args': [
            (['--force'], {'action': 'store_true', 'help': 'Skip confirmation prompt'})
        ]
    },
    'backup': {
        'func': cmd_database_backup,
        'help': 'Create database backup',
        'args': [
            (['--name'], {'help': 'Backup name (default: config_timestamp)'})
        ]
    },
    'vacuum': {
        'func': cmd_database_vacuum,
        'help': 'Optimize database storage',
        'args': []
    },
    'query': {
        'func': cmd_database_query,
        'help': 'Execute custom SQL query',
        'args': [
            (['--query'], {'required': True, 'help': 'SQL query to execute'})
        ]
    }
} 