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

# configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# import config
from config import config

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
        
        # create output directory
        self.data_dir = f"data/validation"
        os.makedirs(self.data_dir, exist_ok=True)
        
        # output files
        self.results_file = f"{self.data_dir}/{chain_id}_valset_validation_results.csv"
        self.evidence_file = f"{self.data_dir}/{chain_id}_valset_evidence_commands.txt"
        self.failure_log_file = f"{self.data_dir}/{chain_id}_valset_validation_failures.log"
        self.state_file = f"{self.data_dir}/{chain_id}_valset_validation_state.json"
        
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
        if not os.path.exists(self.results_file):
            with open(self.results_file, 'w', newline='') as f:
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
                    'evidence_generated'
                ])

    def load_validation_state(self) -> Dict[str, Any]:
        """Load validation state from file"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                    logger.info(f"Resuming from state: {state['total_validations']} validations completed, "
                               f"last tx: {state.get('last_tx_hash', 'none')}")
                    return state
            except Exception as e:
                logger.warning(f"Could not load validation state: {e}")
        
        # default state
        return {
            "last_tx_hash": None,
            "last_block_number": 0,
            "total_validations": 0,
            "last_validation_timestamp": None
        }
    
    def save_validation_state(self, state: Dict[str, Any]):
        """Save validation state to file"""
        try:
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save validation state: {e}")
    
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
        """Load checkpoint data from checkpoint_scribe"""
        checkpoint_file = f"data/layer_checkpoints/{self.chain_id}_checkpoints.csv"
        
        if not os.path.exists(checkpoint_file):
            raise FileNotFoundError(f"Checkpoint data not found: {checkpoint_file}")
        
        checkpoints = []
        with open(checkpoint_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                checkpoints.append({
                    'index': int(row['validator_index']),
                    'timestamp': int(row['validator_timestamp']),
                    'power_threshold': int(row['power_threshold']),
                    'validator_set_hash': row['validator_set_hash'],
                    'checkpoint': row['checkpoint']
                })
        
        # sort by timestamp for efficient searching
        checkpoints.sort(key=lambda x: x['timestamp'])
        logger.info(f"Loaded {len(checkpoints)} checkpoints")
        return checkpoints
    
    def load_valset_data(self) -> List[Dict[str, Any]]:
        """Load validator set update events from valset_watcher"""
        valset_file = f"data/valset/valset_updates.csv"  # note: may need to adjust for multi-chain
        
        if not os.path.exists(valset_file):
            raise FileNotFoundError(f"Validator set data not found: {valset_file}")
        
        valset_updates = []
        with open(valset_file, 'r') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                try:
                    valset_updates.append({
                        'timestamp': row['timestamp'],
                        'block_number': int(row['block_number']),
                        'tx_hash': row['tx_hash'],
                        'log_index': int(row['log_index']),
                        'power_threshold': int(row['power_threshold']),
                        'validator_timestamp': int(row['validator_timestamp']),
                        'validator_set_hash': row['validator_set_hash']
                    })
                except Exception as e:
                    logger.warning(f"Skipping corrupted row {i+2}: {e}")
                    continue
        
        logger.info(f"Loaded {len(valset_updates)} validator set updates")
        return valset_updates
    
    def query_layer_checkpoint_params(self, timestamp: int) -> Optional[Dict[str, Any]]:
        """
        Query Layer to get validator checkpoint params for a specific timestamp
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
            trace_result = self.w3.manager.request_blocking("debug_traceTransaction", [tx_hash, {"tracer": "callTracer"}])
            
            # look for updateValidatorSet call in the trace
            def find_update_validator_set_call(call_trace):
                if call_trace.get("type") == "CALL":
                    input_data = call_trace.get("input", "")
                    if input_data.startswith(UPDATE_VALIDATOR_SET_SELECTOR):
                        return call_trace
                
                # recursively check calls
                for call in call_trace.get("calls", []):
                    result = find_update_validator_set_call(call)
                    if result:
                        return result
                return None
            
            update_call = find_update_validator_set_call(trace_result)
            if not update_call:
                logger.warning(f"No updateValidatorSet call found in transaction {tx_hash}")
                return None
            
            # decode the input data
            input_data = update_call["input"]
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
            'evidence_generated': False
        }
        
        try:
            # step 1: query Layer for checkpoint params at this timestamp
            logger.debug(f"Validating valset update from tx {valset_update['tx_hash']}")
            
            layer_params = self.query_layer_checkpoint_params(valset_update['validator_timestamp'])
            
            if not layer_params:
                # try to find closest checkpoint in our local data
                closest_checkpoint = self.find_closest_checkpoint(valset_update['validator_timestamp'], checkpoints)
                if closest_checkpoint:
                    result['layer_power_threshold'] = closest_checkpoint['power_threshold']
                    result['layer_validator_timestamp'] = closest_checkpoint['timestamp']
                    result['layer_validator_set_hash'] = closest_checkpoint['validator_set_hash']
                    
                    # check if timestamps are close enough (within 1 minute = 60000ms)
                    time_diff = abs(closest_checkpoint['timestamp'] - valset_update['validator_timestamp'])
                    if time_diff > 60000:
                        result['error_details'] = f'No exact Layer checkpoint, closest is {time_diff}ms away'
                        result['status'] = 'NO_LAYER_DATA'
                        return result
                else:
                    result['error_details'] = 'No Layer checkpoint data found'
                    result['status'] = 'NO_LAYER_DATA'
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
        """Write validation result to CSV"""
        try:
            with open(self.results_file, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    result['validation_timestamp'],
                    result['tx_hash'],
                    result['block_number'],
                    result['evm_power_threshold'],
                    result['evm_validator_timestamp'],
                    result['evm_validator_set_hash'],
                    result['layer_power_threshold'],
                    result['layer_validator_timestamp'],
                    result['layer_validator_set_hash'],
                    result['power_threshold_match'],
                    result['timestamp_match'],
                    result['hash_match'],
                    result['status'],
                    result['error_details'],
                    result['evidence_generated']
                ])
                f.flush()
        except Exception as e:
            logger.error(f"Failed to write result: {e}")
    
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
        timestamp_mismatch_count = 0
        no_data_count = 0
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
            elif result['status'] == 'TIMESTAMP_MISMATCH':
                timestamp_mismatch_count += 1
                logger.info(f"⚠️  Timestamp mismatch: {valset_update['tx_hash']}")
            elif result['status'] == 'NO_LAYER_DATA':
                no_data_count += 1
            else:
                error_count += 1
            
            processed_count += 1
            
            # update state after each successful validation
            state["last_tx_hash"] = valset_update["tx_hash"]
            state["last_block_number"] = valset_update["block_number"]
            state["total_validations"] = state["total_validations"] + 1
            state["last_validation_timestamp"] = datetime.utcnow().isoformat()
            self.save_validation_state(state)
            
            # small delay to avoid overwhelming services
            time.sleep(0.2)
        
        # summary
        logger.info(f"Validator set validation complete:")
        logger.info(f"  📊 Processed: {processed_count} new updates")
        logger.info(f"  ✅ Valid: {valid_count}")
        logger.info(f"  🚨 Malicious: {malicious_count}")
        logger.info(f"  ⚠️  Timestamp mismatches: {timestamp_mismatch_count}")
        logger.info(f"  📭 No Layer data: {no_data_count}")
        logger.info(f"  ❌ Errors: {error_count}")
        logger.info(f"  📈 Total validations: {state['total_validations']}")
        logger.info(f"Results saved to: {self.results_file}")
        logger.info(f"State saved to: {self.state_file}")