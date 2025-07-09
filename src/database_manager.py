"""
Database manager for bridge monitoring data using DuckDB.

This module provides a unified interface for storing and querying
bridge monitoring data using DuckDB databases. Each monitoring 
configuration gets its own database file.
"""

import duckdb
import json
import os
import logging
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class BridgeMonitorDB:
    """Database manager for bridge monitoring data"""
    
    def __init__(self, config_name: str, database_path: str):
        """
        Initialize database manager for a specific monitoring configuration
        
        Args:
            config_name: Name of the monitoring configuration
            database_path: Path to the DuckDB database file
        """
        self.config_name = config_name
        self.database_path = database_path
        
        # ensure database directory exists
        os.makedirs(os.path.dirname(database_path), exist_ok=True)
        
        # initialize database schema
        self.init_database()
        
        logger.info(f"Initialized BridgeMonitorDB for config '{config_name}' at {database_path}")
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = duckdb.connect(self.database_path)
        try:
            yield conn
        finally:
            conn.close()
    
    def init_database(self):
        """Initialize database schema if it doesn't exist"""
        with self.get_connection() as conn:
            # enable JSON extension
            conn.execute("INSTALL json; LOAD json;")
            
            # configuration metadata table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS monitoring_config (
                    config_name VARCHAR PRIMARY KEY,
                    display_name VARCHAR,
                    layer_chain VARCHAR,
                    evm_chain VARCHAR,
                    bridge_contract VARCHAR,
                    layer_rpc_url VARCHAR,
                    evm_rpc_url VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # component state table (replaces JSON state files)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS component_state (
                    component_name VARCHAR PRIMARY KEY,
                    state_data JSON,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # layer checkpoints table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS layer_checkpoints (
                    scrape_timestamp TIMESTAMP,
                    validator_index INTEGER,
                    validator_timestamp BIGINT,
                    power_threshold BIGINT,
                    validator_set_hash VARCHAR,
                    checkpoint VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # layer validator sets table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS layer_validator_sets (
                    scrape_timestamp TIMESTAMP,
                    valset_timestamp BIGINT,
                    valset_checkpoint VARCHAR,
                    ethereum_address VARCHAR,
                    power BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # evm valset updates table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS evm_valset_updates (
                    timestamp TIMESTAMP,
                    block_number BIGINT,
                    tx_hash VARCHAR,
                    from_address VARCHAR,
                    to_address VARCHAR,
                    input_data TEXT,
                    gas_used BIGINT,
                    trace_address JSON,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # evm attestations table  
            conn.execute("""
                CREATE TABLE IF NOT EXISTS evm_attestations (
                    timestamp TIMESTAMP,
                    block_number BIGINT,
                    tx_hash VARCHAR,
                    from_address VARCHAR,
                    to_address VARCHAR,
                    input_data TEXT,
                    gas_used BIGINT,
                    trace_address JSON,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # valset validation results table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS valset_validation_results (
                    timestamp TIMESTAMP,
                    block_number BIGINT,
                    tx_hash VARCHAR,
                    validation_status VARCHAR,
                    error_message TEXT,
                    validator_count INTEGER,
                    valid_signatures INTEGER,
                    invalid_signatures INTEGER,
                    layer_checkpoint VARCHAR,
                    evm_checkpoint VARCHAR,
                    is_malicious BOOLEAN,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # attestation validation results table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS attestation_validation_results (
                    timestamp TIMESTAMP,
                    block_number BIGINT,
                    tx_hash VARCHAR,
                    validation_status VARCHAR,
                    error_message TEXT,
                    oracle_data_hash VARCHAR,
                    layer_checkpoint VARCHAR,
                    snapshot_exists BOOLEAN,
                    is_malicious BOOLEAN,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # evidence commands table (replaces text files)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS evidence_commands (
                    evidence_type VARCHAR,
                    tx_hash VARCHAR,
                    validator_address VARCHAR,
                    command_text TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # create indexes for better query performance
            self._create_indexes(conn)
            
        logger.info("Database schema initialized successfully")
    
    def _create_indexes(self, conn):
        """Create indexes for better query performance"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_layer_checkpoints_timestamp ON layer_checkpoints(validator_timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_layer_checkpoints_hash ON layer_checkpoints(validator_set_hash)",
            "CREATE INDEX IF NOT EXISTS idx_layer_validator_sets_timestamp ON layer_validator_sets(valset_timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_evm_valset_block ON evm_valset_updates(block_number)",
            "CREATE INDEX IF NOT EXISTS idx_evm_attestations_block ON evm_attestations(block_number)",
            "CREATE INDEX IF NOT EXISTS idx_valset_validation_tx ON valset_validation_results(tx_hash)",
            "CREATE INDEX IF NOT EXISTS idx_attestation_validation_tx ON attestation_validation_results(tx_hash)",
            "CREATE INDEX IF NOT EXISTS idx_evidence_tx ON evidence_commands(tx_hash)"
        ]
        
        for index_sql in indexes:
            try:
                conn.execute(index_sql)
            except Exception as e:
                logger.warning(f"Failed to create index: {e}")
    
    # Configuration management
    def set_config_metadata(self, display_name: str, layer_chain: str, evm_chain: str, 
                           bridge_contract: str, layer_rpc_url: str, evm_rpc_url: str):
        """Store configuration metadata"""
        with self.get_connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO monitoring_config 
                (config_name, display_name, layer_chain, evm_chain, bridge_contract, layer_rpc_url, evm_rpc_url, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, [self.config_name, display_name, layer_chain, evm_chain, bridge_contract, layer_rpc_url, evm_rpc_url])
    
    # State management (replaces JSON state files)
    def get_component_state(self, component_name: str) -> Dict[str, Any]:
        """Get component state data"""
        with self.get_connection() as conn:
            result = conn.execute(
                "SELECT state_data FROM component_state WHERE component_name = ?",
                [component_name]
            ).fetchone()
            
            if result:
                return json.loads(result[0]) if result[0] else {}
            return {}
    
    def update_component_state(self, component_name: str, **kwargs):
        """Update component state data"""
        # get existing state
        current_state = self.get_component_state(component_name)
        
        # merge with new data
        current_state.update(kwargs)
        
        # save back to database
        with self.get_connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO component_state (component_name, state_data, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            """, [component_name, json.dumps(current_state)])
    
    # Layer data methods (replaces checkpoint_scribe CSV operations)
    def insert_layer_checkpoint(self, data: Dict[str, Any]):
        """Insert a layer checkpoint record"""
        with self.get_connection() as conn:
            conn.execute("""
                INSERT INTO layer_checkpoints 
                (scrape_timestamp, validator_index, validator_timestamp, power_threshold, validator_set_hash, checkpoint)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [
                data['scrape_timestamp'],
                data['validator_index'], 
                data['validator_timestamp'],
                data['power_threshold'],
                data['validator_set_hash'],
                data['checkpoint']
            ])
    
    def insert_layer_validator_set(self, data: Dict[str, Any]):
        """Insert a layer validator set record"""
        with self.get_connection() as conn:
            conn.execute("""
                INSERT INTO layer_validator_sets
                (scrape_timestamp, valset_timestamp, valset_checkpoint, ethereum_address, power)
                VALUES (?, ?, ?, ?, ?)
            """, [
                data['scrape_timestamp'],
                data['valset_timestamp'],
                data['valset_checkpoint'], 
                data['ethereum_address'],
                data['power']
            ])
    
    def get_layer_checkpoint_by_timestamp(self, timestamp: int) -> Optional[Dict[str, Any]]:
        """Get layer checkpoint by validator timestamp"""
        with self.get_connection() as conn:
            result = conn.execute("""
                SELECT scrape_timestamp, validator_index, validator_timestamp, power_threshold, validator_set_hash, checkpoint
                FROM layer_checkpoints 
                WHERE validator_timestamp = ?
                ORDER BY scrape_timestamp DESC
                LIMIT 1
            """, [timestamp]).fetchone()
            
            if result:
                return {
                    'scrape_timestamp': result[0],
                    'validator_index': result[1], 
                    'validator_timestamp': result[2],
                    'power_threshold': result[3],
                    'validator_set_hash': result[4],
                    'checkpoint': result[5]
                }
            return None
    
    def get_validator_set_by_timestamp(self, timestamp: int) -> List[Dict[str, Any]]:
        """Get validator set by valset timestamp"""
        with self.get_connection() as conn:
            results = conn.execute("""
                SELECT scrape_timestamp, valset_timestamp, valset_checkpoint, ethereum_address, power
                FROM layer_validator_sets
                WHERE valset_timestamp = ?
                ORDER BY power DESC
            """, [timestamp]).fetchall()
            
            return [{
                'scrape_timestamp': row[0],
                'valset_timestamp': row[1],
                'valset_checkpoint': row[2], 
                'ethereum_address': row[3],
                'power': row[4]
            } for row in results]
    
    # EVM data methods (replaces watcher CSV operations)
    def insert_evm_valset_update(self, data: Dict[str, Any]):
        """Insert an EVM valset update record"""
        with self.get_connection() as conn:
            conn.execute("""
                INSERT INTO evm_valset_updates
                (timestamp, block_number, tx_hash, from_address, to_address, input_data, gas_used, trace_address)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                data['timestamp'],
                data['block_number'],
                data['tx_hash'],
                data['from_address'],
                data['to_address'], 
                data['input_data'],
                data['gas_used'],
                json.dumps(data['trace_address'])
            ])
    
    def insert_evm_attestation(self, data: Dict[str, Any]):
        """Insert an EVM attestation record"""
        with self.get_connection() as conn:
            conn.execute("""
                INSERT INTO evm_attestations
                (timestamp, block_number, tx_hash, from_address, to_address, input_data, gas_used, trace_address)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                data['timestamp'],
                data['block_number'],
                data['tx_hash'],
                data['from_address'],
                data['to_address'],
                data['input_data'],
                data['gas_used'], 
                json.dumps(data['trace_address'])
            ])
    
    # Validation methods (replaces verifier CSV operations)
    def insert_valset_validation_result(self, data: Dict[str, Any]):
        """Insert a valset validation result"""
        with self.get_connection() as conn:
            conn.execute("""
                INSERT INTO valset_validation_results
                (timestamp, block_number, tx_hash, validation_status, error_message, validator_count, 
                 valid_signatures, invalid_signatures, layer_checkpoint, evm_checkpoint, is_malicious)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                data['timestamp'],
                data['block_number'],
                data['tx_hash'],
                data['validation_status'],
                data.get('error_message'),
                data.get('validator_count'),
                data.get('valid_signatures'),
                data.get('invalid_signatures'),
                data.get('layer_checkpoint'),
                data.get('evm_checkpoint'),
                data.get('is_malicious', False)
            ])
    
    def insert_attestation_validation_result(self, data: Dict[str, Any]):
        """Insert an attestation validation result"""
        with self.get_connection() as conn:
            conn.execute("""
                INSERT INTO attestation_validation_results
                (timestamp, block_number, tx_hash, validation_status, error_message, oracle_data_hash,
                 layer_checkpoint, snapshot_exists, is_malicious)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                data['timestamp'],
                data['block_number'], 
                data['tx_hash'],
                data['validation_status'],
                data.get('error_message'),
                data.get('oracle_data_hash'),
                data.get('layer_checkpoint'),
                data.get('snapshot_exists'),
                data.get('is_malicious', False)
            ])
    
    def insert_evidence_command(self, data: Dict[str, Any]):
        """Insert an evidence command"""
        with self.get_connection() as conn:
            conn.execute("""
                INSERT INTO evidence_commands
                (evidence_type, tx_hash, validator_address, command_text)
                VALUES (?, ?, ?, ?)
            """, [
                data['evidence_type'],
                data['tx_hash'],
                data.get('validator_address'),
                data['command_text']
            ])
    
    # Query methods
    def get_statistics(self) -> Dict[str, Any]:
        """Get database statistics"""
        with self.get_connection() as conn:
            stats = {}
            
            # table counts
            tables = ['layer_checkpoints', 'layer_validator_sets', 'evm_valset_updates', 
                     'evm_attestations', 'valset_validation_results', 'attestation_validation_results', 
                     'evidence_commands']
            
            for table in tables:
                result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                stats[f'{table}_count'] = result[0] if result else 0
            
            # latest timestamps
            try:
                result = conn.execute("SELECT MAX(validator_timestamp) FROM layer_checkpoints").fetchone()
                stats['latest_checkpoint_timestamp'] = result[0] if result[0] else None
                
                result = conn.execute("SELECT MAX(block_number) FROM evm_valset_updates").fetchone()
                stats['latest_valset_block'] = result[0] if result[0] else None
                
                result = conn.execute("SELECT MAX(block_number) FROM evm_attestations").fetchone()
                stats['latest_attestation_block'] = result[0] if result[0] else None
            except Exception as e:
                logger.warning(f"Failed to get latest timestamps: {e}")
            
            return stats
    
    def get_recent_malicious_activity(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent malicious activity"""
        with self.get_connection() as conn:
            # combine malicious valset and attestation results
            results = conn.execute("""
                SELECT 'valset' as type, timestamp, tx_hash, validation_status, error_message
                FROM valset_validation_results 
                WHERE is_malicious = true
                UNION ALL
                SELECT 'attestation' as type, timestamp, tx_hash, validation_status, error_message  
                FROM attestation_validation_results
                WHERE is_malicious = true
                ORDER BY timestamp DESC
                LIMIT ?
            """, [limit]).fetchall()
            
            return [{
                'type': row[0],
                'timestamp': row[1],
                'tx_hash': row[2],
                'validation_status': row[3],
                'error_message': row[4]
            } for row in results]
    
    def query_evm_attestations(self, filters: Optional[Dict[str, Any]] = None, 
                              limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Query EVM attestations with optional filters"""
        with self.get_connection() as conn:
            sql = "SELECT timestamp, block_number, tx_hash, from_address, to_address, input_data, gas_used, trace_address FROM evm_attestations"
            params = []
            
            if filters:
                conditions = []
                if 'block_number' in filters:
                    conditions.append("block_number = ?")
                    params.append(filters['block_number'])
                if 'tx_hash' in filters:
                    conditions.append("tx_hash = ?")
                    params.append(filters['tx_hash'])
                if 'from_block' in filters:
                    conditions.append("block_number >= ?")
                    params.append(filters['from_block'])
                if 'to_block' in filters:
                    conditions.append("block_number <= ?")
                    params.append(filters['to_block'])
                
                if conditions:
                    sql += " WHERE " + " AND ".join(conditions)
            
            sql += " ORDER BY block_number DESC"
            
            if limit:
                sql += f" LIMIT {limit}"
            
            results = conn.execute(sql, params).fetchall()
            
            return [{
                'timestamp': row[0],
                'block_number': row[1],
                'tx_hash': row[2],
                'from_address': row[3],
                'to_address': row[4],
                'input_data': row[5],
                'gas_used': row[6],
                'trace_address': json.loads(row[7]) if row[7] else []
            } for row in results]
    
    def backup_database(self, backup_path: str):
        """Create a backup of the database"""
        with self.get_connection() as conn:
            conn.execute(f"EXPORT DATABASE '{backup_path}' (FORMAT PARQUET)")
        logger.info(f"Database backed up to {backup_path}")
    
    def vacuum_database(self):
        """Optimize database by reclaiming space"""
        with self.get_connection() as conn:
            conn.execute("VACUUM")
        logger.info("Database vacuumed successfully") 