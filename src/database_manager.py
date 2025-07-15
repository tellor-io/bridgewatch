"""
Database manager for bridge monitoring data using DuckDB.

This module provides a unified interface for storing and querying
bridge monitoring data using DuckDB databases. Each monitoring 
configuration gets its own database file.

Enhanced with database lock handling and deduplication features.
"""

import duckdb
import json
import os
import logging
import time
import random
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class DatabaseLockError(Exception):
    """Raised when database is locked by another process"""
    pass

class BridgeMonitorDB:
    """Database manager for bridge monitoring data with enhanced lock handling"""
    
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
    
    def is_database_lock_error(self, error: Exception) -> bool:
        """Check if error is a DuckDB lock conflict"""
        error_str = str(error).lower()
        return (
            "could not set lock on file" in error_str or
            "conflicting lock is held" in error_str or
            "database is locked" in error_str or
            "io error" in error_str and "lock" in error_str
        )
    
    @contextmanager
    def get_connection_with_retry(self, max_retries: int = 8, base_delay: float = 1.0):
        """Context manager for database connections with retry on lock conflicts"""
        for attempt in range(max_retries):
            try:
                conn = duckdb.connect(self.database_path)
                try:
                    yield conn
                    return  # success, exit retry loop
                finally:
                    conn.close()
            except Exception as e:
                if self.is_database_lock_error(e) and attempt < max_retries - 1:
                    # exponential backoff with jitter
                    delay = base_delay * (2 ** attempt) * (1 + random.uniform(-0.1, 0.1))
                    logger.warning(f"Database locked, retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(delay)
                    continue
                else:
                    # not a lock error or out of retries
                    if self.is_database_lock_error(e):
                        raise DatabaseLockError(f"Database locked after {max_retries} attempts: {e}")
                    else:
                        raise

    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = duckdb.connect(self.database_path)
        try:
            yield conn
        finally:
            conn.close()
    
    def init_database(self):
        """Initialize database schema with unique constraints for deduplication"""
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
            
            # layer checkpoints table with unique constraint
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
            
            # add unique constraint for layer checkpoints if not exists
            try:
                conn.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_layer_checkpoints_unique 
                    ON layer_checkpoints(validator_timestamp, checkpoint)
                """)
            except Exception as e:
                logger.debug(f"Layer checkpoints unique index may already exist: {e}")
            
            # layer validator sets table with unique constraint
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
            
            # add unique constraint for layer validator sets if not exists
            try:
                conn.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_layer_validator_sets_unique 
                    ON layer_validator_sets(valset_timestamp, valset_checkpoint, ethereum_address)
                """)
            except Exception as e:
                logger.debug(f"Layer validator sets unique index may already exist: {e}")
            
            # evm valset updates table with unique constraint
            conn.execute("""
                CREATE TABLE IF NOT EXISTS evm_valset_updates (
                    timestamp TIMESTAMP,
                    block_number BIGINT,
                    tx_hash VARCHAR,
                    log_index INTEGER,
                    power_threshold BIGINT,
                    validator_timestamp BIGINT,
                    validator_set_hash VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # add unique constraint for evm valset updates if not exists
            try:
                conn.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_evm_valset_updates_unique 
                    ON evm_valset_updates(tx_hash, log_index)
                """)
            except Exception as e:
                logger.debug(f"EVM valset updates unique index may already exist: {e}")
            
            # evm attestations table with unique constraint
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
            
            # add unique constraint for evm attestations if not exists  
            try:
                conn.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_evm_attestations_unique 
                    ON evm_attestations(tx_hash, trace_address)
                """)
            except Exception as e:
                logger.debug(f"EVM attestations unique index may already exist: {e}")
            
            # valset validation results table with unique constraint
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
            
            # add unique constraint for valset validation results if not exists
            try:
                conn.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_valset_validation_unique 
                    ON valset_validation_results(tx_hash)
                """)
            except Exception as e:
                logger.debug(f"Valset validation unique index may already exist: {e}")
            
            # attestation validation results table with unique constraint
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
            
            # add unique constraint for attestation validation results if not exists
            try:
                conn.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_attestation_validation_unique 
                    ON attestation_validation_results(tx_hash)
                """)
            except Exception as e:
                logger.debug(f"Attestation validation unique index may already exist: {e}")
            
            # evidence commands table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS evidence_commands (
                    evidence_type VARCHAR,
                    tx_hash VARCHAR,
                    validator_address VARCHAR,
                    command_text TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # add unique constraint for evidence commands if not exists
            try:
                conn.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_evidence_commands_unique 
                    ON evidence_commands(evidence_type, tx_hash, validator_address)
                """)
            except Exception as e:
                logger.debug(f"Evidence commands unique index may already exist: {e}")
            
            # create indexes for better query performance
            self._create_indexes(conn)
            
        logger.info("Database schema with deduplication constraints initialized successfully")
    
    def _create_indexes(self, conn):
        """Create indexes for better query performance"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_layer_checkpoints_timestamp ON layer_checkpoints(validator_timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_layer_checkpoints_hash ON layer_checkpoints(validator_set_hash)",
            "CREATE INDEX IF NOT EXISTS idx_layer_validator_sets_timestamp ON layer_validator_sets(valset_timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_evm_valset_block ON evm_valset_updates(block_number)",
            "CREATE INDEX IF NOT EXISTS idx_evm_valset_timestamp ON evm_valset_updates(validator_timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_evm_valset_hash ON evm_valset_updates(validator_set_hash)",
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
        try:
            with self.get_connection_with_retry() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO monitoring_config 
                    (config_name, display_name, layer_chain, evm_chain, bridge_contract, layer_rpc_url, evm_rpc_url, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, [self.config_name, display_name, layer_chain, evm_chain, bridge_contract, layer_rpc_url, evm_rpc_url])
        except DatabaseLockError:
            logger.warning(f"Could not set config metadata for {self.config_name} due to database lock")
            raise

    # State management (replaces JSON state files)
    def get_component_state(self, component_name: str) -> Dict[str, Any]:
        """Get component state data"""
        try:
            with self.get_connection_with_retry() as conn:
                result = conn.execute(
                    "SELECT state_data FROM component_state WHERE component_name = ?",
                    [component_name]
                ).fetchone()
                
                if result:
                    return json.loads(result[0]) if result[0] else {}
                return {}
        except DatabaseLockError:
            logger.warning(f"Could not get component state for {component_name} due to database lock")
            raise

    def update_component_state(self, component_name: str, **kwargs):
        """Update component state data"""
        # get existing state
        current_state = self.get_component_state(component_name)
        
        # merge with new data
        current_state.update(kwargs)
        
        # save back to database
        self.save_component_state(component_name, current_state)

    def save_component_state(self, component_name: str, state: Dict[str, Any]):
        """Save complete component state data"""
        try:
            with self.get_connection_with_retry() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO component_state (component_name, state_data, updated_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                """, [component_name, json.dumps(state)])
        except DatabaseLockError:
            logger.warning(f"Could not save component state for {component_name} due to database lock")
            raise
    
    # Enhanced data insertion methods with deduplication
    def insert_layer_checkpoint(self, data: Dict[str, Any]):
        """Insert a layer checkpoint record with deduplication"""
        try:
            with self.get_connection_with_retry() as conn:
                conn.execute("""
                    INSERT OR IGNORE INTO layer_checkpoints 
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
        except DatabaseLockError:
            logger.warning(f"Skipping checkpoint insert due to database lock: timestamp {data['validator_timestamp']}")
            raise

    def insert_layer_validator_set(self, data: Dict[str, Any]):
        """Insert a layer validator set record with deduplication"""
        try:
            with self.get_connection_with_retry() as conn:
                conn.execute("""
                    INSERT OR IGNORE INTO layer_validator_sets
                    (scrape_timestamp, valset_timestamp, valset_checkpoint, ethereum_address, power)
                    VALUES (?, ?, ?, ?, ?)
                """, [
                    data['scrape_timestamp'],
                    data['valset_timestamp'],
                    data['valset_checkpoint'], 
                    data['ethereum_address'],
                    data['power']
                ])
        except DatabaseLockError:
            logger.warning(f"Skipping validator set insert due to database lock: {data['ethereum_address']}")
            raise
    
    def get_layer_checkpoint_by_timestamp(self, timestamp: int) -> Optional[Dict[str, Any]]:
        """Get layer checkpoint by validator timestamp"""
        try:
            with self.get_connection_with_retry() as conn:
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
        except DatabaseLockError:
            logger.warning(f"Could not get layer checkpoint for timestamp {timestamp} due to database lock")
            raise
    
    def get_validator_set_by_timestamp(self, timestamp: int) -> List[Dict[str, Any]]:
        """Get validator set by valset timestamp"""
        try:
            with self.get_connection_with_retry() as conn:
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
        except DatabaseLockError:
            logger.warning(f"Could not get validator set for timestamp {timestamp} due to database lock")
            raise
    
    def get_all_layer_checkpoints(self) -> List[Dict[str, Any]]:
        """Get all layer checkpoints"""
        try:
            with self.get_connection_with_retry() as conn:
                results = conn.execute("""
                    SELECT scrape_timestamp, validator_index, validator_timestamp, power_threshold, validator_set_hash, checkpoint
                    FROM layer_checkpoints 
                    ORDER BY validator_timestamp ASC
                """).fetchall()
                
                return [{
                    'scrape_timestamp': row[0],
                    'validator_index': row[1],
                    'validator_timestamp': row[2],
                    'power_threshold': row[3],
                    'validator_set_hash': row[4],
                    'checkpoint': row[5]
                } for row in results]
        except DatabaseLockError:
            logger.warning(f"Could not get all layer checkpoints due to database lock")
            raise
    
    def get_all_evm_valset_updates(self) -> List[Dict[str, Any]]:
        """Get all EVM validator set updates"""
        try:
            with self.get_connection_with_retry() as conn:
                results = conn.execute("""
                    SELECT timestamp, block_number, tx_hash, log_index, power_threshold, validator_timestamp, validator_set_hash
                    FROM evm_valset_updates 
                    ORDER BY timestamp ASC
                """).fetchall()
                
                return [{
                    'timestamp': row[0],
                    'block_number': row[1],
                    'tx_hash': row[2],
                    'log_index': row[3],
                    'power_threshold': row[4],
                    'validator_timestamp': row[5],
                    'validator_set_hash': row[6]
                } for row in results]
        except DatabaseLockError:
            logger.warning(f"Could not get all EVM valset updates due to database lock")
            raise
    
    def get_all_evm_attestations(self) -> List[Dict[str, Any]]:
        """Get all EVM attestations"""
        try:
            with self.get_connection_with_retry() as conn:
                results = conn.execute("""
                    SELECT timestamp, block_number, tx_hash, from_address, to_address, input_data, gas_used, trace_address
                    FROM evm_attestations 
                    ORDER BY timestamp ASC
                """).fetchall()
                
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
        except DatabaseLockError:
            logger.warning(f"Could not get all EVM attestations due to database lock")
            raise
    
    def get_layer_validator_set_by_checkpoint(self, checkpoint: str) -> List[Dict[str, Any]]:
        """Get validator set by checkpoint hash"""
        try:
            with self.get_connection_with_retry() as conn:
                results = conn.execute("""
                    SELECT scrape_timestamp, valset_timestamp, valset_checkpoint, ethereum_address, power
                    FROM layer_validator_sets
                    WHERE valset_checkpoint = ?
                    ORDER BY power DESC
                """, [checkpoint]).fetchall()
                
                return [{
                    'scrape_timestamp': row[0],
                    'valset_timestamp': row[1],
                    'valset_checkpoint': row[2], 
                    'ethereum_address': row[3],
                    'power': row[4]
                } for row in results]
        except DatabaseLockError:
            logger.warning(f"Could not get validator set for checkpoint {checkpoint} due to database lock")
            raise
    
    def get_layer_validator_set_by_timestamp(self, timestamp: int) -> List[Dict[str, Any]]:
        """Get validator set by timestamp (alias for get_validator_set_by_timestamp)"""
        return self.get_validator_set_by_timestamp(timestamp)
    
    # EVM data methods (replaces watcher CSV operations)
    def insert_evm_valset_update(self, data: Dict[str, Any]):
        """Insert an EVM valset update record"""
        try:
            with self.get_connection_with_retry() as conn:
                conn.execute("""
                    INSERT OR IGNORE INTO evm_valset_updates
                    (timestamp, block_number, tx_hash, log_index, power_threshold, validator_timestamp, validator_set_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, [
                    data['timestamp'],
                    data['block_number'],
                    data['tx_hash'],
                    data['log_index'],
                    data['power_threshold'],
                    data['validator_timestamp'],
                    data['validator_set_hash']
                ])
        except DatabaseLockError:
            logger.warning(f"Skipping EVM valset update insert due to database lock: block {data['block_number']}")
            raise

    def insert_evm_attestation(self, data: Dict[str, Any]):
        """Insert an EVM attestation record"""
        try:
            with self.get_connection_with_retry() as conn:
                conn.execute("""
                    INSERT OR IGNORE INTO evm_attestations
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
        except DatabaseLockError:
            logger.warning(f"Skipping EVM attestation insert due to database lock: block {data['block_number']}")
            raise
    
    # Validation methods (replaces verifier CSV operations)
    def insert_valset_validation_result(self, data: Dict[str, Any]):
        """Insert a valset validation result"""
        try:
            with self.get_connection_with_retry() as conn:
                conn.execute("""
                    INSERT OR IGNORE INTO valset_validation_results
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
        except DatabaseLockError:
            logger.warning(f"Skipping valset validation result insert due to database lock: block {data['block_number']}")
            raise

    def insert_attestation_validation_result(self, data: Dict[str, Any]):
        """Insert an attestation validation result"""
        try:
            with self.get_connection_with_retry() as conn:
                conn.execute("""
                    INSERT OR IGNORE INTO attestation_validation_results
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
        except DatabaseLockError:
            logger.warning(f"Skipping attestation validation result insert due to database lock: block {data['block_number']}")
            raise
    
    def insert_evidence_command(self, data: Dict[str, Any]):
        """Insert an evidence command"""
        try:
            with self.get_connection_with_retry() as conn:
                conn.execute("""
                    INSERT OR IGNORE INTO evidence_commands
                    (evidence_type, tx_hash, validator_address, command_text)
                    VALUES (?, ?, ?, ?)
                """, [
                    data['evidence_type'],
                    data['tx_hash'],
                    data.get('validator_address'),
                    data['command_text']
                ])
        except DatabaseLockError:
            logger.warning(f"Skipping evidence command insert due to database lock: tx_hash {data['tx_hash']}")
            raise
    
    # Query methods
    def get_statistics(self) -> Dict[str, Any]:
        """Get database statistics"""
        try:
            with self.get_connection_with_retry() as conn:
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
        except DatabaseLockError:
            logger.warning(f"Could not get database statistics due to database lock")
            raise
    
    def get_recent_malicious_activity(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent malicious activity"""
        try:
            with self.get_connection_with_retry() as conn:
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
        except DatabaseLockError:
            logger.warning(f"Could not get recent malicious activity due to database lock")
            raise
    
    def query_evm_attestations(self, filters: Optional[Dict[str, Any]] = None, 
                              limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Query EVM attestations with optional filters"""
        try:
            with self.get_connection_with_retry() as conn:
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
        except DatabaseLockError:
            logger.warning(f"Could not query EVM attestations due to database lock")
            raise
    
    def backup_database(self, backup_path: str):
        """Create a backup of the database"""
        try:
            with self.get_connection_with_retry() as conn:
                conn.execute(f"EXPORT DATABASE '{backup_path}' (FORMAT PARQUET)")
            logger.info(f"Database backed up to {backup_path}")
        except DatabaseLockError:
            logger.warning(f"Could not backup database to {backup_path} due to database lock")
            raise
    
    def vacuum_database(self):
        """Optimize database by reclaiming space"""
        try:
            with self.get_connection_with_retry() as conn:
                conn.execute("VACUUM")
            logger.info("Database vacuumed successfully")
        except DatabaseLockError:
            logger.warning(f"Could not vacuum database due to database lock")
            raise 