#!/usr/bin/env python3
"""
Valset Verifier

This component validates ValidatorSetUpdated events by:
1. Loading valset update events from valset_watcher.py
2. Loading checkpoint data from checkpoint_scribe.py  
3. Querying Layer to get validator checkpoint params for each timestamp
4. Comparing Layer params vs EVM event params
5. Detecting mismatches → malicious validator set updates
6. For malicious updates: tracing transactions and generating evidence commands

A "verifier" emphasizes analysis and truth-checking against Layer.
"""

import json
import csv
import time
import requests
from typing import List, Dict, Any, Optional, Tuple
from web3 import Web3
from eth_abi import decode
import logging
from datetime import datetime
import os
import hashlib
from eth_utils import decode_hex
from eth_keys import keys

# configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# import config
from config import config, get_config_manager

# configuration from config module
DEFAULT_LAYER_RPC_URL = config.get_layer_rpc_url()
DEFAULT_EVM_RPC_URL = config.get_evm_rpc_url()
UPDATE_VALIDATOR_SET_SELECTOR = config.get_function_selector('updateValidatorSet')
MAX_RETRIES = config.get_max_retries()
RETRY_DELAY = config.get_retry_delay()

class ValsetVerifier:
    def __init__(self, layer_rpc_url: str, evm_rpc_url: str, chain_id: str):
        self.layer_rpc_url = layer_rpc_url.rstrip('/')
        self.evm_rpc_url = evm_rpc_url
        self.chain_id = chain_id
        self.w3 = Web3(Web3.HTTPProvider(evm_rpc_url))
        
        # test connection
        if not self.w3.is_connected():
            raise ConnectionError("Failed to connect to EVM RPC")
        
        # get config manager for directory paths and database
        try:
            config_manager = get_config_manager()
            self.data_dir = config_manager.get_validation_dir()
            self.checkpoint_dir = config_manager.get_layer_checkpoints_dir()
            self.valset_dir = config_manager.get_valset_dir()
            
            # get Discord webhook URL from config
            self.discord_webhook_url = config_manager.get_discord_webhook_url()
            
            # initialize database connection
            self.db = config_manager.create_database_manager()
            self.db.init_database()
        except RuntimeError:
            # fallback to legacy paths if in legacy mode
            self.data_dir = f"data/validation"
            self.checkpoint_dir = f"data/layer_checkpoints"
            self.valset_dir = f"data/valset"
            self.discord_webhook_url = os.getenv('DISCORD_WEBHOOK_URL')
            self.db = None
        
        # create data directory
        os.makedirs(self.data_dir, exist_ok=True)
        
        # output files
        self.state_file = f"{self.data_dir}/{chain_id}_valset_validation_state.json"
        self.validation_csv_file = f"{self.data_dir}/{chain_id}_valset_validation_results.csv"
        self.evidence_file = f"{self.data_dir}/{chain_id}_valset_evidence_commands.txt"
        self.failure_log_file = f"{self.data_dir}/{chain_id}_valset_validation_failures.log"
        
        # initialize CSV file
        self.init_results_csv()
        
        # test connection to Layer
        self.test_layer_connection()
    
    def test_layer_connection(self):
        """Test connection to Layer RPC"""
        try:
            url = f"{self.layer_rpc_url}/layer/bridge/get_current_validator_set_timestamp"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            logger.info(f"Connected to Layer RPC: {self.layer_rpc_url}")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Layer RPC: {e}")
    
    def init_results_csv(self):
        """Initialize results CSV file"""
        if not os.path.exists(self.validation_csv_file):
            with open(self.validation_csv_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'validation_timestamp',
                    'tx_hash',
                    'block_number',
                    'evm_power_threshold',
                    'evm_validator_timestamp', 
                    'evm_validator_set_hash',
                    'layer_power_threshold',
                    'layer_validator_timestamp',
                    'layer_validator_set_hash',
                    'power_threshold_match',
                    'timestamp_match',
                    'hash_match',
                    'status',
                    'error_details',
                    'evidence_generated',
                    'is_valid_validator_signature',
                    'signing_validator_count',
                    'signing_power_percentage',
                    'malicious_checkpoint_signed'
                ])

    def load_validation_state(self) -> Dict[str, Any]:
        """Load validation state from database"""
        try:
            state = self.db.get_component_state('valset_verifier')
            if state:
                logger.info(f"Resuming from database state: {state.get('total_validations', 0)} validations completed, "
                           f"last tx: {state.get('last_tx_hash', 'none')}")
                return state
            else:
                logger.info("No previous validation state found in database, starting fresh")
                # default state
                return {
                    "last_tx_hash": None,
                    "last_block_number": 0,
                    "total_validations": 0,
                    "last_validation_timestamp": None
                }
        except Exception as e:
            logger.warning(f"Could not load validation state from database: {e}")
            # default state
            return {
                "last_tx_hash": None,
                "last_block_number": 0,
                "total_validations": 0,
                "last_validation_timestamp": None
            }
    
    def save_validation_state(self, state: Dict[str, Any]):
        """Save validation state to database"""
        try:
            self.db.save_component_state('valset_verifier', state)
            logger.debug(f"Saved validation state to database: {state}")
        except Exception as e:
            logger.error(f"Could not save validation state to database: {e}")
    
    def filter_new_valset_updates(self, valset_updates: List[Dict[str, Any]], state: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Filter valset updates to only include new ones since last validation"""
        if not state.get("last_tx_hash"):
            # first run, process all
            return valset_updates
        
        last_tx_hash = state["last_tx_hash"]
        last_block_number = state["last_block_number"]
        
        # find the index of the last processed transaction
        last_index = -1
        for i, valset_update in enumerate(valset_updates):
            if valset_update["tx_hash"] == last_tx_hash and valset_update["block_number"] == last_block_number:
                last_index = i
                break
        
        if last_index >= 0:
            # return everything after the last processed item
            new_updates = valset_updates[last_index + 1:]
            logger.info(f"Found {len(new_updates)} new valset updates since last run (skipping {last_index + 1})")
            return new_updates
        else:
            # couldn't find last processed item, maybe data was modified
            logger.warning(f"Could not find last processed transaction {last_tx_hash} in current data, processing all")
            return valset_updates
    
    def load_checkpoint_data(self) -> List[Dict[str, Any]]:
        """Load checkpoint data from database"""
        try:
            # get all checkpoints from database
            checkpoints = self.db.get_all_layer_checkpoints()
            
            # convert to the expected format
            formatted_checkpoints = []
            for checkpoint in checkpoints:
                formatted_checkpoints.append({
                    'index': checkpoint['validator_index'],
                    'timestamp': checkpoint['validator_timestamp'],
                    'power_threshold': checkpoint['power_threshold'],
                    'validator_set_hash': checkpoint['validator_set_hash'],
                    'checkpoint': checkpoint['checkpoint']
                })
            
            # sort by timestamp for efficient searching
            formatted_checkpoints.sort(key=lambda x: x['timestamp'])
            logger.info(f"Loaded {len(formatted_checkpoints)} checkpoints from database")
            return formatted_checkpoints
        except Exception as e:
            logger.error(f"Failed to load checkpoint data from database: {e}")
            return []

    def load_valset_data(self) -> List[Dict[str, Any]]:
        """Load validator set update events from database"""
        try:
            # get all valset updates from database
            valset_updates = self.db.get_all_evm_valset_updates()
            logger.info(f"Loaded {len(valset_updates)} validator set updates from database")
            return valset_updates
        except Exception as e:
            logger.error(f"Failed to load valset data from database: {e}")
            return []

    def lookup_validator_set_by_timestamp(self, timestamp: int) -> Optional[List[Dict[str, Any]]]:
        """
        Lookup validator set by timestamp from database
        """
        try:
            # get validator set from database
            validator_set = self.db.get_layer_validator_set_by_timestamp(timestamp)
            
            if validator_set:
                # convert to the expected format
                formatted_validators = []
                for validator in validator_set:
                    formatted_validators.append({
                        'ethereumAddress': validator['ethereum_address'],
                        'power': validator['power'],
                        'scrape_timestamp': validator['scrape_timestamp'],
                        'valset_timestamp': validator['valset_timestamp']
                    })
                
                logger.debug(f"Found validator set for timestamp {timestamp}: {len(formatted_validators)} validators")
                return formatted_validators
            else:
                logger.debug(f"No validator set found for timestamp {timestamp}")
                return None
                
        except Exception as e:
            logger.error(f"Error loading validator set for timestamp {timestamp}: {e}")
            return None

    def query_layer_checkpoint_params(self, timestamp: int) -> Optional[Dict[str, Any]]:
        """
        Query Layer to get validator checkpoint params for a specific timestamp
        Returns None if no checkpoint exists (indicates malicious data was signed)
        """
        try:
            url = f"{self.layer_rpc_url}/layer/bridge/get_validator_checkpoint_params/{timestamp}"
            response = requests.get(url, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                logger.debug(f"Layer checkpoint for timestamp {timestamp}: {data}")
                return data
            elif response.status_code == 404:
                logger.debug(f"No Layer checkpoint found for timestamp {timestamp}")
                return None
            elif response.status_code == 500:
                # check if this is the specific "failed to get validator checkpoint params" error
                try:
                    error_data = response.json()
                    if error_data.get('code') == 13 and 'failed to get validator checkpoint params' in error_data.get('message', ''):
                        logger.warning(f"Layer confirms no checkpoint exists for timestamp {timestamp} - potential malicious signature")
                        return None
                except:
                    pass
                logger.warning(f"Layer API error for timestamp {timestamp}: {response.status_code}")
                return None
            else:
                logger.warning(f"Layer API error for timestamp {timestamp}: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to query Layer for timestamp {timestamp}: {e}")
            return None
    
    def find_closest_checkpoint(self, target_timestamp: int, checkpoints: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Find the checkpoint closest to the target timestamp
        """
        closest_checkpoint = None
        min_diff = float('inf')
        
        for checkpoint in checkpoints:
            diff = abs(checkpoint['timestamp'] - target_timestamp)
            if diff < min_diff:
                min_diff = diff
                closest_checkpoint = checkpoint
        
        if closest_checkpoint:
            logger.debug(f"Found closest checkpoint: index {closest_checkpoint['index']}, "
                        f"timestamp {closest_checkpoint['timestamp']} for target {target_timestamp} "
                        f"(diff: {min_diff}ms)")
        
        return closest_checkpoint
    
    def trace_transaction_for_evidence(self, tx_hash: str) -> Optional[Dict[str, Any]]:
        """
        Trace a transaction to extract updateValidatorSet parameters for evidence
        """
        try:
            logger.debug(f"Tracing transaction {tx_hash} for evidence extraction")
            
            # get transaction trace
            trace_result = self.w3.manager.request_blocking("trace_transaction", [tx_hash])
            
            # trace_transaction returns a list of trace objects
            if not isinstance(trace_result, list):
                logger.error(f"Unexpected trace result format for {tx_hash}: {type(trace_result)}")
                return None
            
            # look for updateValidatorSet call in the trace list
            def find_update_validator_set_call_in_traces(traces):
                for trace in traces:
                    if trace.get("type") == "call":
                        action = trace.get("action", {})
                        input_data = action.get("input", "")
                        if input_data.startswith(UPDATE_VALIDATOR_SET_SELECTOR):
                            return trace
                return None
            
            update_call = find_update_validator_set_call_in_traces(trace_result)
            if not update_call:
                logger.warning(f"No updateValidatorSet call found in transaction {tx_hash}")
                return None
            
            # decode the input data
            action = update_call.get("action", {})
            input_data = action.get("input", "")
            decoded_params = self.decode_update_validator_set_calldata(input_data)
            
            if decoded_params:
                logger.debug(f"Successfully traced and decoded parameters for {tx_hash}")
                return decoded_params
            else:
                logger.warning(f"Failed to decode updateValidatorSet parameters for {tx_hash}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to trace transaction {tx_hash}: {e}")
            return None
    
    def decode_update_validator_set_calldata(self, input_data: str) -> Optional[Dict[str, Any]]:
        """
        Decode updateValidatorSet calldata
        
        function updateValidatorSet(
            bytes32 _newValidatorSetHash,
            uint64 _newPowerThreshold,
            uint256 _newValidatorTimestamp,
            Validator[] calldata _currentValidatorSet,
            Signature[] calldata _sigs
        )
        """
        try:
            # remove function selector (first 4 bytes)
            if input_data.startswith('0x'):
                calldata = input_data[10:]  # remove '0x' + 8 chars (4 bytes)
            else:
                calldata = input_data[8:]   # remove 8 chars (4 bytes)
            
            # decode the parameters
            types = [
                'bytes32',  # _newValidatorSetHash
                'uint64',   # _newPowerThreshold
                'uint256',  # _newValidatorTimestamp
                '(address,uint256)[]',  # Validator[] _currentValidatorSet
                '(uint8,bytes32,bytes32)[]'  # Signature[] _sigs
            ]
            
            decoded = decode(types, bytes.fromhex(calldata))
            
            new_validator_set_hash, new_power_threshold, new_validator_timestamp, current_validator_set, signatures = decoded
            
            result = {
                'new_validator_set_hash': '0x' + new_validator_set_hash.hex(),
                'new_power_threshold': new_power_threshold,
                'new_validator_timestamp': new_validator_timestamp,
                'current_validator_set': [{'address': addr, 'power': power} for addr, power in current_validator_set],
                'signatures': [{'v': v, 'r': '0x' + r.hex(), 's': '0x' + s.hex()} for v, r, s in signatures]
            }
            
            logger.debug(f"Decoded updateValidatorSet: new_hash={result['new_validator_set_hash']}, "
                        f"new_power={result['new_power_threshold']}, new_timestamp={result['new_validator_timestamp']}")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to decode updateValidatorSet calldata: {e}")
            return None
    
    def generate_evidence_command(self, tx_hash: str, valset_params: Dict[str, Any], result: Dict[str, Any]) -> str:
        """
        Generate CLI command for submitting validator set evidence
        """
        try:
            # format: ./layerd tx bridge submit-valset-signature-evidence 
            # <creator> <valset_timestamp> <valset_hash> <power_threshold> <signature>
            
            # use the first non-empty signature as evidence
            evidence_signature = None
            evidence_validator = None
            
            for i, sig in enumerate(valset_params['signatures']):
                if sig['v'] != 0 or sig['r'] != '0x' + '00' * 32 or sig['s'] != '0x' + '00' * 32:
                    evidence_signature = sig
                    evidence_validator = valset_params['current_validator_set'][i]
                    break
            
            if not evidence_signature:
                return f"# No valid signature found for evidence in tx {tx_hash}"
            
            # format signature as hex
            sig_hex = f"{evidence_signature['v']:02x}{evidence_signature['r'][2:]}{evidence_signature['s'][2:]}"
            
            # create command
            command = f"""# Evidence for malicious validator set update
# Transaction: {tx_hash}
# EVM params: power={result['evm_power_threshold']}, timestamp={result['evm_validator_timestamp']}, hash={result['evm_validator_set_hash']}
# Layer params: power={result['layer_power_threshold']}, timestamp={result['layer_validator_timestamp']}, hash={result['layer_validator_set_hash']}
# Validator used for evidence: {evidence_validator['address']} (power: {evidence_validator['power']})

./layerd tx bridge submit-valset-signature-evidence \\
  <CREATOR_ADDRESS> \\
  {valset_params['new_validator_timestamp']} \\
  {valset_params['new_validator_set_hash']} \\
  {valset_params['new_power_threshold']} \\
  0x{sig_hex} \\
  --from <CREATOR_ADDRESS> \\
  --keyring-backend test \\
  --chain-id {self.chain_id} \\
  --fees 500loya

"""
            return command
            
        except Exception as e:
            logger.error(f"Failed to generate evidence command for {tx_hash}: {e}")
            return f"# Error generating evidence command for {tx_hash}: {e}"
    
    def write_evidence_command(self, command: str):
        """Write evidence command to file"""
        try:
            with open(self.evidence_file, 'a') as f:
                f.write(command)
                f.write('\n' + '='*80 + '\n\n')
                f.flush()
        except Exception as e:
            logger.error(f"Failed to write evidence command: {e}")
    
    def validate_valset_update(self, valset_update: Dict[str, Any], checkpoints: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate a single validator set update
        """
        result = {
            'validation_timestamp': datetime.utcnow().isoformat(),
            'tx_hash': valset_update['tx_hash'],
            'block_number': valset_update['block_number'],
            'evm_power_threshold': valset_update['power_threshold'],
            'evm_validator_timestamp': valset_update['validator_timestamp'],
            'evm_validator_set_hash': valset_update['validator_set_hash'],
            'layer_power_threshold': None,
            'layer_validator_timestamp': None,
            'layer_validator_set_hash': None,
            'power_threshold_match': False,
            'timestamp_match': False,
            'hash_match': False,
            'status': 'FAILED',
            'error_details': '',
            'evidence_generated': False,
            'is_valid_validator_signature': False,
            'signing_validator_count': 0,
            'signing_power_percentage': 0,
            'malicious_checkpoint_signed': False
        }
        
        try:
            # step 1: query Layer for checkpoint params at this timestamp
            logger.debug(f"Validating valset update from tx {valset_update['tx_hash']}")
            
            layer_params = self.query_layer_checkpoint_params(valset_update['validator_timestamp'])
            
            if not layer_params:
                # CRITICAL: No checkpoint exists for this timestamp on Layer blockchain
                # This means validators signed a non-existent checkpoint → MALICIOUS BEHAVIOR
                logger.warning(f"🚨 NO LAYER CHECKPOINT EXISTS for timestamp {valset_update['validator_timestamp']} - MALICIOUS SIGNATURE DETECTED")
                
                # trace transaction to extract signature parameters
                valset_params = self.trace_transaction_for_evidence(valset_update['tx_hash'])
                if not valset_params:
                    result['error_details'] = 'No Layer checkpoint exists but failed to extract transaction data for signature verification'
                    result['status'] = 'MALICIOUS_NO_SIGNATURE_DATA'
                    result['malicious_checkpoint_signed'] = True
                    return result
                
                # find the latest real checkpoint before this malicious timestamp for validator set lookup
                latest_checkpoint = self.find_latest_checkpoint_before(valset_update['validator_timestamp'], checkpoints)
                if not latest_checkpoint:
                    result['error_details'] = 'No Layer checkpoint exists and no previous checkpoint found for validator lookup'
                    result['status'] = 'MALICIOUS_NO_VALIDATOR_DATA'
                    result['malicious_checkpoint_signed'] = True
                    return result
                
                # load validator set from latest checkpoint before malicious timestamp
                validator_set = self.lookup_validator_set_by_timestamp(latest_checkpoint['timestamp'])
                if not validator_set:
                    result['error_details'] = f'No Layer checkpoint exists and failed to load validator set for checkpoint {latest_checkpoint["checkpoint"]}'
                    result['status'] = 'MALICIOUS_NO_VALIDATOR_DATA'
                    result['malicious_checkpoint_signed'] = True
                    return result
                
                # verify signatures against real validators
                signature_verification = self.verify_valset_signatures_against_validators(valset_params, validator_set)
                
                if signature_verification['is_valid_validator']:
                    # signatures came from real validators → CONFIRMED MALICIOUS BEHAVIOR
                    result['status'] = 'MALICIOUS_CHECKPOINT_SIGNED'
                    result['error_details'] = f'Validators signed non-existent checkpoint (timestamp: {valset_update["validator_timestamp"]})'
                    result['is_valid_validator_signature'] = True
                    result['signing_validator_count'] = len(signature_verification['verified_signatures'])
                    result['signing_power_percentage'] = signature_verification['signing_percentage']
                    result['malicious_checkpoint_signed'] = True
                    
                    # send Discord alert for malicious behavior
                    self.send_discord_alert("malicious_valset_signature", {
                        'tx_hash': valset_update['tx_hash'],
                        'block_number': valset_update['block_number'],
                        'validator_timestamp': valset_update['validator_timestamp'],
                        'signing_validator_count': len(signature_verification['verified_signatures']),
                        'signing_power_percentage': signature_verification['signing_percentage'],
                        'verified_signatures': signature_verification['verified_signatures']
                    })
                    
                    logger.error(f"🚨 CONFIRMED MALICIOUS BEHAVIOR: {len(signature_verification['verified_signatures'])} validators signed non-existent checkpoint")
                    logger.error(f"   Malicious validators: {[sig['address'] for sig in signature_verification['verified_signatures']]}")
                    logger.error(f"   Total malicious power: {signature_verification['signing_percentage']:.2f}%")
                    
                    # generate evidence for malicious signatures
                    evidence_command = self.generate_evidence_command(valset_update['tx_hash'], valset_params, result)
                    self.write_evidence_command(evidence_command)
                    result['evidence_generated'] = True
                    
                    return result
                else:
                    # signatures not from validators → might be spam or invalid
                    result['status'] = 'INVALID_SIGNATURES'
                    result['error_details'] = 'No Layer checkpoint exists and signatures not from valid validators (likely spam)'
                    result['is_valid_validator_signature'] = False
                    result['signing_validator_count'] = 0
                    result['signing_power_percentage'] = 0
                    result['malicious_checkpoint_signed'] = False
                    return result
            else:
                # extract layer params from API response and normalize format
                result['layer_power_threshold'] = int(layer_params.get('power_threshold', 0))
                result['layer_validator_timestamp'] = int(layer_params.get('timestamp', 0))
                # add 0x prefix to hash if not present
                valset_hash = layer_params.get('valset_hash', '')
                if valset_hash and not valset_hash.startswith('0x'):
                    valset_hash = '0x' + valset_hash
                result['layer_validator_set_hash'] = valset_hash
            
            # step 2: compare EVM vs Layer parameters
            power_match = result['evm_power_threshold'] == result['layer_power_threshold']
            timestamp_match = result['evm_validator_timestamp'] == result['layer_validator_timestamp']
            hash_match = result['evm_validator_set_hash'] == result['layer_validator_set_hash']
            
            result['power_threshold_match'] = power_match
            result['timestamp_match'] = timestamp_match
            result['hash_match'] = hash_match
            
            # step 3: determine status
            if power_match and timestamp_match and hash_match:
                result['status'] = 'VALID'
            elif not power_match or not hash_match:
                result['status'] = 'MALICIOUS'
                mismatches = []
                if not power_match:
                    mismatches.append(f"power_threshold: EVM={result['evm_power_threshold']} vs Layer={result['layer_power_threshold']}")
                if not hash_match:
                    mismatches.append(f"validator_set_hash: EVM={result['evm_validator_set_hash']} vs Layer={result['layer_validator_set_hash']}")
                result['error_details'] = '; '.join(mismatches)
                
                # step 4: trace transaction and generate evidence for malicious updates
                logger.warning(f"🚨 MALICIOUS VALIDATOR SET UPDATE DETECTED: {valset_update['tx_hash']}")
                logger.warning(f"   Details: {result['error_details']}")
                
                valset_params = self.trace_transaction_for_evidence(valset_update['tx_hash'])
                if valset_params:
                    evidence_command = self.generate_evidence_command(valset_update['tx_hash'], valset_params, result)
                    self.write_evidence_command(evidence_command)
                    result['evidence_generated'] = True
                    logger.info(f"📝 Evidence command generated for {valset_update['tx_hash']}")
                else:
                    logger.error(f"❌ Failed to generate evidence for {valset_update['tx_hash']}")
                    
            elif not timestamp_match:
                result['status'] = 'TIMESTAMP_MISMATCH'
                result['error_details'] = f"timestamp: EVM={result['evm_validator_timestamp']} vs Layer={result['layer_validator_timestamp']}"
            

            
        except Exception as e:
            result['error_details'] = str(e)
            logger.error(f"Error validating valset update {valset_update['tx_hash']}: {e}")
        
        return result
    
    def write_result(self, result: Dict[str, Any]):
        """Write validation result to database"""
        try:
            # prepare data for database insertion
            data = {
                'timestamp': datetime.fromisoformat(result['validation_timestamp'].replace('Z', '+00:00')),
                'tx_hash': result['tx_hash'],
                'block_number': result['block_number'],
                'evm_power_threshold': result['evm_power_threshold'],
                'evm_validator_timestamp': result['evm_validator_timestamp'],
                'evm_validator_set_hash': result['evm_validator_set_hash'],
                'layer_power_threshold': result['layer_power_threshold'],
                'layer_validator_timestamp': result['layer_validator_timestamp'],
                'layer_validator_set_hash': result['layer_validator_set_hash'],
                'power_threshold_match': result['power_threshold_match'],
                'timestamp_match': result['timestamp_match'],
                'hash_match': result['hash_match'],
                'validation_status': result['status'],
                'error_message': result['error_details'],
                'evidence_generated': result['evidence_generated'],
                'is_malicious': result['malicious_checkpoint_signed'],
                'validator_signature_valid': result['is_valid_validator_signature'],
                'signing_validator_count': result['signing_validator_count'],
                'signing_power_percentage': result['signing_power_percentage']
            }
            
            self.db.insert_valset_validation_result(data)
            logger.info(f"Saved valset validation result for tx {result['tx_hash']}")
            
        except Exception as e:
            logger.error(f"Failed to write validation result to database: {e}")
    
    def validate_all_valset_updates(self):
        """
        Main validation process - validate all validator set updates
        """
        logger.info("Starting validator set update validation process")
        
        # load state
        state = self.load_validation_state()
        
        # load data
        checkpoints = self.load_checkpoint_data()
        all_valset_updates = self.load_valset_data()
        
        if not checkpoints:
            logger.error("No checkpoint data available")
            return
        
        if not all_valset_updates:
            logger.error("No validator set update data available")
            return
        
        # filter to only new valset updates
        valset_updates = self.filter_new_valset_updates(all_valset_updates, state)
        
        if not valset_updates:
            logger.info("No new validator set updates to validate")
            return
        
        logger.info(f"Validating {len(valset_updates)} new validator set updates against {len(checkpoints)} checkpoints")
        
        # validate each valset update
        valid_count = 0
        malicious_count = 0
        malicious_checkpoint_count = 0
        timestamp_mismatch_count = 0
        no_data_count = 0
        invalid_signature_count = 0
        error_count = 0
        processed_count = 0
        
        for i, valset_update in enumerate(valset_updates):
            logger.info(f"Processing valset update {i+1}/{len(valset_updates)}: {valset_update['tx_hash']}")
            
            result = self.validate_valset_update(valset_update, checkpoints)
            self.write_result(result)
            
            # track statistics
            if result['status'] == 'VALID':
                valid_count += 1
            elif result['status'] == 'MALICIOUS':
                malicious_count += 1
                logger.warning(f"🚨 MALICIOUS VALIDATOR SET UPDATE DETECTED: {valset_update['tx_hash']}")
                logger.warning(f"   Details: {result['error_details']}")
            elif result['status'] == 'MALICIOUS_CHECKPOINT_SIGNED':
                malicious_checkpoint_count += 1
                logger.error(f"🚨 MALICIOUS CHECKPOINT SIGNATURE DETECTED: {valset_update['tx_hash']}")
                logger.error(f"   Details: {result['error_details']}")
            elif result['status'] == 'TIMESTAMP_MISMATCH':
                timestamp_mismatch_count += 1
                logger.info(f"⚠️  Timestamp mismatch: {valset_update['tx_hash']}")
            elif result['status'] == 'NO_LAYER_DATA':
                no_data_count += 1
            elif result['status'] == 'INVALID_SIGNATURES':
                invalid_signature_count += 1
                logger.info(f"ℹ️  Invalid signatures (not from validators): {valset_update['tx_hash']}")
            else:
                error_count += 1
            
            processed_count += 1
            
            # update state after each successful validation
            state["last_tx_hash"] = valset_update["tx_hash"]
            state["last_block_number"] = valset_update["block_number"]
            state["total_validations"] = state.get("total_validations", 0) + 1
            state["last_validation_timestamp"] = datetime.utcnow().isoformat()
            self.save_validation_state(state)
            
            # small delay to avoid overwhelming services
            time.sleep(0.2)
        
        # summary
        logger.info(f"Validator set validation complete:")
        logger.info(f"  📊 Processed: {processed_count} new updates")
        logger.info(f"  ✅ Valid: {valid_count}")
        logger.info(f"  🚨 Malicious (param mismatch): {malicious_count}")
        logger.info(f"  🔥 Malicious (non-existent checkpoint): {malicious_checkpoint_count}")
        logger.info(f"  ⚠️  Timestamp mismatches: {timestamp_mismatch_count}")
        logger.info(f"  📧 Invalid signatures (spam): {invalid_signature_count}")
        logger.info(f"  📭 No Layer data: {no_data_count}")
        logger.info(f"  ❌ Errors: {error_count}")
        logger.info(f"  📈 Total validations: {state['total_validations']}")
        
        total_malicious = malicious_count + malicious_checkpoint_count
        if total_malicious > 0:
            logger.warning(f"⚠️  {total_malicious} MALICIOUS VALIDATOR SET UPDATES DETECTED - check evidence file")
            logger.warning(f"   Evidence file: {self.evidence_file}")
            
            if malicious_checkpoint_count > 0:
                logger.error(f"🔥 CRITICAL: {malicious_checkpoint_count} validators signed NON-EXISTENT checkpoints!")
        
        logger.info(f"Results saved to: {self.validation_csv_file}")
        logger.info(f"State saved to: {self.state_file}")

    def find_latest_checkpoint_before(self, target_timestamp: int, checkpoints: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Find the latest checkpoint with timestamp before the target timestamp
        This is used for validator set lookup when detecting malicious signatures
        """
        latest_checkpoint = None
        
        for checkpoint in checkpoints:
            if checkpoint['timestamp'] < target_timestamp:
                if not latest_checkpoint or checkpoint['timestamp'] > latest_checkpoint['timestamp']:
                    latest_checkpoint = checkpoint
        
        if latest_checkpoint:
            logger.debug(f"Found latest checkpoint before {target_timestamp}: index {latest_checkpoint['index']}, "
                        f"timestamp {latest_checkpoint['timestamp']}")
        
        return latest_checkpoint

    def ecrecover_signature_with_validator_check(self, message_hash: bytes, signature: Dict[str, Any], 
                                                validator_addresses: set) -> Optional[str]:
        """
        Recover Ethereum address from signature and message hash, trying both v values (27 and 28)
        Returns the address only if it matches a validator in the set
        """
        try:
            # extract signature components
            r = signature.get('r', '0x0')
            s = signature.get('s', '0x0')
            
            # skip empty signatures
            if r == '0x0000000000000000000000000000000000000000000000000000000000000000':
                return None
            
            # convert r and s to bytes
            r_bytes = decode_hex(r)
            s_bytes = decode_hex(s)
            
            # try both v values (27 and 28) since cosmos-sdk doesn't provide v
            for v_recovery in [0, 1]:  # eth_keys uses 0,1 instead of 27,28
                try:
                    # create signature object
                    signature_obj = keys.Signature(vrs=(
                        v_recovery,
                        int.from_bytes(r_bytes, byteorder='big'),
                        int.from_bytes(s_bytes, byteorder='big')
                    ))
                    
                    # recover public key from signature and message hash
                    public_key = signature_obj.recover_public_key_from_msg_hash(message_hash)
                    
                    # get address from public key
                    address = public_key.to_checksum_address()
                    
                    # check if this recovered address is in the validator set
                    if address.lower() in validator_addresses:
                        logger.debug(f"Valid signature recovered with v={v_recovery+27}: {address}")
                        return address
                    else:
                        logger.debug(f"Recovered address {address} (v={v_recovery+27}) not in validator set, trying next v value")
                    
                except Exception as e:
                    logger.debug(f"Failed to recover with v={v_recovery+27}: {e}")
                    continue
            
            # no valid validator address found with either v value
            logger.debug("No valid validator address recovered with either v=27 or v=28")
            return None
            
        except Exception as e:
            logger.debug(f"Error recovering signature: {e}")
            return None

    def verify_valset_signatures_against_validators(self, valset_params: Dict[str, Any], 
                                                  validator_set: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Verify if any signature in the valset update was signed by a validator in the set
        Uses checkpoint hash calculation for signature verification
        """
        try:
            # calculate checkpoint hash for signature verification
            # checkpoint = keccak256(abi.encode(timestamp, valsetHash, powerThreshold))
            timestamp = valset_params['new_validator_timestamp']
            valset_hash = valset_params['new_validator_set_hash']
            power_threshold = valset_params['new_power_threshold']
            
            # encode for checkpoint hash
            from eth_abi import encode
            domain_separator = "0x636865636b706f696e7400000000000000000000000000000000000000000000"
            encoded = encode(['bytes32', 'uint256', 'uint256', 'bytes32'], 
                           [decode_hex(domain_separator), power_threshold, timestamp, decode_hex(valset_hash)])
            checkpoint_hash = Web3.keccak(encoded)
            
            # sha256 hash the checkpoint as mentioned by user
            message_hash = hashlib.sha256(checkpoint_hash).digest()
            
            # create validator address lookup
            validator_addresses = {v['ethereumAddress'].lower() for v in validator_set}
            total_validator_power = sum(v['power'] for v in validator_set)
            
            verified_signatures = []
            total_signing_power = 0
            
            for i, signature in enumerate(valset_params['signatures']):
                # try to recover signature with both v values and check against validator set
                recovered_address = self.ecrecover_signature_with_validator_check(
                    message_hash, signature, validator_addresses
                )
                
                if recovered_address:
                    # find the validator's power
                    validator_power = next(
                        (v['power'] for v in validator_set if v['ethereumAddress'].lower() == recovered_address.lower()),
                        0
                    )
                    
                    verified_signatures.append({
                        'index': i,
                        'address': recovered_address,
                        'power': validator_power
                    })
                    total_signing_power += validator_power
                    
                    logger.debug(f"Valid signature from validator {recovered_address} with power {validator_power}")
                else:
                    logger.debug(f"Signature {i} not from valid validator (tried both v=27 and v=28)")
            
            is_valid = len(verified_signatures) > 0
            
            return {
                'is_valid_validator': is_valid,
                'verified_signatures': verified_signatures,
                'total_signing_power': total_signing_power,
                'total_validator_power': total_validator_power,
                'signing_percentage': (total_signing_power / total_validator_power * 100) if total_validator_power > 0 else 0
            }
            
        except Exception as e:
            logger.error(f"Error verifying valset signatures against validators: {e}")
            return {
                'is_valid_validator': False,
                'verified_signatures': [],
                'total_signing_power': 0,
                'total_validator_power': 0,
                'signing_percentage': 0
            }

    def send_discord_alert(self, alert_type: str, details: Dict[str, Any]):
        """Send Discord alert for malicious validator set signatures"""
        if not self.discord_webhook_url:
            logger.debug("No Discord webhook URL configured, skipping alert")
            return
        
        try:
            if alert_type == "malicious_valset_signature":
                embed = {
                    "title": "🚨 MALICIOUS VALIDATOR SET SIGNATURE DETECTED",
                    "color": 16711680,  # red
                    "fields": [
                        {"name": "Transaction Hash", "value": f"`{details['tx_hash']}`", "inline": False},
                        {"name": "Block Number", "value": str(details['block_number']), "inline": True},
                        {"name": "Validator Timestamp", "value": str(details['validator_timestamp']), "inline": True},
                        {"name": "Signing Validators", "value": str(details['signing_validator_count']), "inline": True},
                        {"name": "Signing Power", "value": f"{details['signing_power_percentage']:.2f}%", "inline": True},
                        {"name": "Issue", "value": "Validators signed non-existent checkpoint", "inline": False},
                        {"name": "Validator Addresses", "value": "\n".join([f"`{sig['address']}` (power: {sig['power']})" 
                                                                            for sig in details['verified_signatures'][:5]]), "inline": False}
                    ],
                    "timestamp": datetime.utcnow().isoformat()
                }
            else:
                return  # unknown alert type
            
            payload = {
                "embeds": [embed]
            }
            
            response = requests.post(self.discord_webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            logger.info(f"Discord alert sent successfully for {alert_type}")
            
        except Exception as e:
            logger.error(f"Failed to send Discord alert: {e}")

def main():
    verifier = ValsetVerifier(
            layer_rpc_url=config.get_layer_rpc_url(),
            evm_rpc_url=config.get_evm_rpc_url(),
            chain_id=config.get_chain_id()
    )
    verifier.validate_all_valset_updates()

if __name__ == "__main__":
    main()