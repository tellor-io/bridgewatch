#!/usr/bin/env python3
"""
Attest Verifier

This component validates attestations by:
1. Decoding verifyOracleData calldata 
2. Finding matching checkpoint for attestationTimestamp
3. Calculating attestation snapshot
4. Querying Layer to verify snapshot exists
5. Validating signature against validator set

A "verifier" emphasizes analysis and truth-checking against Layer.
"""

import json
import csv
import time
import argparse
import requests
from typing import List, Dict, Any, Optional, Tuple
from web3 import Web3
from eth_abi import decode
import logging
from datetime import datetime
import os
import hashlib

# configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# import config
from config import config

# configuration from config module
DEFAULT_LAYER_RPC_URL = config.get_layer_rpc_url()
NEW_REPORT_ATTESTATION_DOMAIN_SEPARATOR = config.get_domain_separator('new_report_attestation')
VALIDATOR_SET_HASH_DOMAIN_SEPARATOR = config.get_domain_separator('validator_set_hash')
VERIFY_ORACLE_DATA_SELECTOR = config.get_function_selector('verifyOracleData')

class AttestVerifier:
    def __init__(self, layer_rpc_url: str, chain_id: str):
        self.layer_rpc_url = layer_rpc_url.rstrip('/')
        self.chain_id = chain_id
        self.w3 = Web3()  # for ABI decoding only
        
        # create output directory
        self.data_dir = f"data/validation"
        os.makedirs(self.data_dir, exist_ok=True)
        
        # output files
        self.results_file = f"{self.data_dir}/{chain_id}_attestation_validation_results.csv"
        self.failure_log_file = f"{self.data_dir}/{chain_id}_attestation_validation_failures.log"
        self.state_file = f"{self.data_dir}/{chain_id}_attestation_validation_state.json"
        
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
                    'attestation_timestamp',
                    'query_id',
                    'value_hash',
                    'report_timestamp',
                    'aggregate_power',
                    'snapshot',
                    'checkpoint_used',
                    'checkpoint_timestamp',
                    'layer_snapshot_exists',
                    'signature_valid',
                    'status',
                    'error_details'
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
    
    def filter_new_attestations(self, attestations: List[Dict[str, Any]], state: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Filter attestations to only include new ones since last validation"""
        if not state.get("last_tx_hash"):
            # first run, process all
            return attestations
        
        last_tx_hash = state["last_tx_hash"]
        last_block_number = state["last_block_number"]
        
        # find the index of the last processed transaction
        last_index = -1
        for i, attestation in enumerate(attestations):
            if attestation["tx_hash"] == last_tx_hash and attestation["block_number"] == last_block_number:
                last_index = i
                break
        
        if last_index >= 0:
            # return everything after the last processed item
            new_attestations = attestations[last_index + 1:]
            logger.info(f"Found {len(new_attestations)} new attestations since last run (skipping {last_index + 1})")
            return new_attestations
        else:
            # couldn't find last processed item, maybe data was modified
            logger.warning(f"Could not find last processed transaction {last_tx_hash} in current data, processing all")
            return attestations
    
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
    
    def load_attestation_data(self) -> List[Dict[str, Any]]:
        """Load attestation calldata from attest_watcher"""
        attestation_file = f"data/oracle/attestations.csv"  # note: may need to adjust for multi-chain
        
        if not os.path.exists(attestation_file):
            raise FileNotFoundError(f"Attestation data not found: {attestation_file}")
        
        attestations = []
        with open(attestation_file, 'r') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                try:
                    attestations.append({
                        'timestamp': row['timestamp'],
                        'block_number': int(row['block_number']),
                        'tx_hash': row['tx_hash'],
                        'from_address': row['from_address'],
                        'to_address': row['to_address'],
                        'input_data': row['input_data'],
                        'gas_used': row['gas_used'],
                        'trace_address': json.loads(row['trace_address'])
                    })
                except Exception as e:
                    logger.warning(f"Skipping corrupted row {i+2}: {e}")
                    continue
        
        logger.info(f"Loaded {len(attestations)} attestation calls")
        return attestations
    
    def decode_verify_oracle_data_calldata(self, input_data: str) -> Optional[Dict[str, Any]]:
        """
        Decode verifyOracleData calldata to extract attestation parameters
        
        function verifyOracleData(
            OracleAttestationData calldata _attestData,
            Validator[] calldata _currentValidatorSet,
            Signature[] calldata _sigs
        )
        
        struct OracleAttestationData {
            bytes32 queryId;
            ReportData report;
            uint256 attestationTimestamp;
        }
        
        struct ReportData {
            bytes value;
            uint256 timestamp;
            uint256 aggregatePower;
            uint256 previousTimestamp;
            uint256 nextTimestamp;
            uint256 lastConsensusTimestamp;
        }
        """
        try:
            # remove function selector (first 4 bytes)
            if input_data.startswith('0x'):
                calldata = input_data[10:]  # remove '0x' + 8 chars (4 bytes)
            else:
                calldata = input_data[8:]   # remove 8 chars (4 bytes)
            
            # decode the top-level parameters
            # the function has 3 parameters: attestData (tuple), validators (array), signatures (array)
            types = [
                '(bytes32,(bytes,uint256,uint256,uint256,uint256,uint256),uint256)',  # OracleAttestationData
                '(address,uint256)[]',  # Validator[]
                '(uint8,bytes32,bytes32)[]'  # Signature[]
            ]
            
            decoded = decode(types, bytes.fromhex(calldata))
            
            # extract attestation data
            attest_data, validators, signatures = decoded
            
            # unpack OracleAttestationData
            query_id, report_data, attestation_timestamp = attest_data
            
            # unpack ReportData
            value, timestamp, aggregate_power, previous_timestamp, next_timestamp, last_consensus_timestamp = report_data
            
            result = {
                'query_id': '0x' + query_id.hex(),
                'value': '0x' + value.hex() if value else '0x',
                'timestamp': timestamp,
                'aggregate_power': aggregate_power,
                'previous_timestamp': previous_timestamp,
                'next_timestamp': next_timestamp,
                'last_consensus_timestamp': last_consensus_timestamp,
                'attestation_timestamp': attestation_timestamp,
                'validators': [{'address': addr, 'power': power} for addr, power in validators],
                'signatures': [{'v': v, 'r': '0x' + r.hex(), 's': '0x' + s.hex()} for v, r, s in signatures]
            }
            
            logger.debug(f"Decoded attestation: query_id={result['query_id']}, "
                        f"timestamp={result['timestamp']}, attestation_timestamp={result['attestation_timestamp']}")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to decode calldata: {e}")
            logger.debug(f"Input data length: {len(input_data)}, first 100 chars: {input_data[:100]}")
            return None
    
    def find_matching_checkpoint(self, attestation_timestamp: int, checkpoints: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Find the checkpoint with timestamp <= attestation_timestamp (closest match)
        """
        matching_checkpoint = None
        
        for checkpoint in checkpoints:
            # checkpoint timestamps are in milliseconds, attestation might be in seconds
            # we need to normalize this - let's assume both are in milliseconds for now
            if checkpoint['timestamp'] <= attestation_timestamp:
                matching_checkpoint = checkpoint
            else:
                break  # checkpoints are sorted, so we can stop here
        
        if matching_checkpoint:
            logger.debug(f"Found matching checkpoint: index {matching_checkpoint['index']}, "
                        f"timestamp {matching_checkpoint['timestamp']} for attestation {attestation_timestamp}")
        else:
            logger.warning(f"No matching checkpoint found for attestation timestamp {attestation_timestamp}")
        
        return matching_checkpoint
    
    def calculate_attestation_snapshot(self, attestation_data: Dict[str, Any], checkpoint: str) -> str:
        """
        Calculate attestation snapshot using the same encoding as the bridge contract
        
        bytes32 _dataDigest = keccak256(
            abi.encode(
                NEW_REPORT_ATTESTATION_DOMAIN_SEPARATOR,
                _attestData.queryId,
                _attestData.report.value,
                _attestData.report.timestamp,
                _attestData.report.aggregatePower,
                _attestData.report.previousTimestamp,
                _attestData.report.nextTimestamp,
                lastValidatorSetCheckpoint,
                _attestData.attestationTimestamp,
                _attestData.report.lastConsensusTimestamp
            )
        );
        """
        try:
            from eth_abi import encode
            
            # extract values from decoded attestation data
            query_id = attestation_data['query_id']
            value = attestation_data['value']
            timestamp = attestation_data['timestamp']
            aggregate_power = attestation_data['aggregate_power']
            previous_timestamp = attestation_data['previous_timestamp']
            next_timestamp = attestation_data['next_timestamp']
            attestation_timestamp = attestation_data['attestation_timestamp']
            last_consensus_timestamp = attestation_data['last_consensus_timestamp']
            
            # convert hex strings to bytes
            domain_separator = bytes.fromhex(NEW_REPORT_ATTESTATION_DOMAIN_SEPARATOR[2:])
            query_id_bytes = bytes.fromhex(query_id[2:])
            value_bytes = bytes.fromhex(value[2:]) if value != '0x' else b''
            checkpoint_bytes = bytes.fromhex(checkpoint[2:] if checkpoint.startswith('0x') else checkpoint)
            
            # encode all parameters using eth_abi
            encoded_data = encode([
                'bytes32',  # domain separator
                'bytes32',  # query_id
                'bytes',    # value
                'uint256',  # timestamp
                'uint256',  # aggregate_power
                'uint256',  # previous_timestamp
                'uint256',  # next_timestamp
                'bytes32',  # checkpoint
                'uint256',  # attestation_timestamp
                'uint256'   # last_consensus_timestamp
            ], [
                domain_separator,
                query_id_bytes,
                value_bytes,
                timestamp,
                aggregate_power,
                previous_timestamp,
                next_timestamp,
                checkpoint_bytes,
                attestation_timestamp,
                last_consensus_timestamp
            ])
            
            # calculate keccak256 hash
            snapshot = Web3.keccak(encoded_data).hex()
            logger.debug(f"Calculated snapshot: {snapshot}")
            logger.debug(f"Parameters: query_id={query_id}, timestamp={timestamp}, "
                        f"attestation_timestamp={attestation_timestamp}, checkpoint={checkpoint[:10]}...")
            return snapshot
            
        except Exception as e:
            logger.error(f"Failed to calculate snapshot: {e}")
            logger.error(f"Attestation data keys: {list(attestation_data.keys())}")
            return ""
    
    def query_layer_snapshot(self, snapshot: str) -> bool:
        """
        Query Layer to check if snapshot exists
        """
        try:
            url = f"{self.layer_rpc_url}/layer/bridge/get_attestation_data_by_snapshot/{snapshot}"
            response = requests.get(url, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                # if we get data back, snapshot exists
                if data and any(data.get(key) for key in ['query_id', 'timestamp', 'aggregate_value']):
                    logger.debug(f"Snapshot {snapshot} exists on Layer")
                    return True
                else:
                    logger.debug(f"Snapshot {snapshot} returned empty data")
                    return False
            else:
                logger.debug(f"Snapshot {snapshot} not found on Layer (status: {response.status_code})")
                return False
                
        except Exception as e:
            logger.error(f"Failed to query Layer for snapshot {snapshot}: {e}")
            return False
    
    def validate_signature(self, attestation_data: Dict[str, Any], validator_set: List[Dict[str, Any]], 
                          signatures: List[Dict[str, Any]], snapshot: str) -> bool:
        """
        Validate the first signature against the validator set
        """
        # placeholder implementation
        # proper implementation would:
        # 1. Extract the first non-empty signature
        # 2. Recover the address from signature + snapshot
        # 3. Check if recovered address is in validator set
        # 4. Check if validator has sufficient power
        
        logger.debug("Signature validation not fully implemented")
        return True  # placeholder
    
    def validate_attestation(self, attestation: Dict[str, Any], checkpoints: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate a single attestation
        """
        result = {
            'validation_timestamp': datetime.utcnow().isoformat(),
            'tx_hash': attestation['tx_hash'],
            'block_number': attestation['block_number'],
            'attestation_timestamp': None,
            'query_id': None,
            'value_hash': None,
            'report_timestamp': None,
            'aggregate_power': None,
            'snapshot': None,
            'checkpoint_used': None,
            'checkpoint_timestamp': None,
            'layer_snapshot_exists': False,
            'signature_valid': False,
            'status': 'FAILED',
            'error_details': ''
        }
        
        try:
            # step 1: decode calldata
            logger.debug(f"Validating attestation from tx {attestation['tx_hash']}")
            
            decoded_data = self.decode_verify_oracle_data_calldata(attestation['input_data'])
            if not decoded_data:
                result['error_details'] = 'Failed to decode calldata'
                return result
            
            # extract key parameters
            attestation_timestamp = decoded_data.get('attestation_timestamp')
            if not attestation_timestamp:
                result['error_details'] = 'Missing attestation_timestamp'
                return result
            
            result['attestation_timestamp'] = attestation_timestamp
            result['query_id'] = decoded_data['query_id']
            result['value_hash'] = Web3.keccak(hexstr=decoded_data['value']).hex()[:10] if decoded_data['value'] != '0x' else '0x0000000000'
            result['report_timestamp'] = decoded_data['timestamp']
            result['aggregate_power'] = decoded_data['aggregate_power']
            
            # step 2: find matching checkpoint
            matching_checkpoint = self.find_matching_checkpoint(attestation_timestamp, checkpoints)
            if not matching_checkpoint:
                result['error_details'] = 'No matching checkpoint found'
                return result
            
            result['checkpoint_used'] = matching_checkpoint['checkpoint']
            result['checkpoint_timestamp'] = matching_checkpoint['timestamp']
            
            # step 3: calculate snapshot
            snapshot = self.calculate_attestation_snapshot(decoded_data, matching_checkpoint['checkpoint'])
            if not snapshot:
                result['error_details'] = 'Failed to calculate snapshot'
                return result
            
            result['snapshot'] = snapshot
            
            # step 4: query Layer
            layer_exists = self.query_layer_snapshot(snapshot)
            result['layer_snapshot_exists'] = layer_exists
            
            if not layer_exists:
                result['status'] = 'MALICIOUS'
                result['error_details'] = 'Snapshot does not exist on Layer'
                return result
            
            # step 5: validate signature (optional)
            sig_valid = self.validate_signature(
                decoded_data, 
                decoded_data.get('validators', []),
                decoded_data.get('signatures', []),
                snapshot
            )
            result['signature_valid'] = sig_valid
            
            if layer_exists:
                result['status'] = 'VALID'
            
        except Exception as e:
            result['error_details'] = str(e)
            logger.error(f"Error validating attestation {attestation['tx_hash']}: {e}")
        
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
                    result['attestation_timestamp'],
                    result['query_id'],
                    result['value_hash'],
                    result['report_timestamp'],
                    result['aggregate_power'],
                    result['snapshot'],
                    result['checkpoint_used'],
                    result['checkpoint_timestamp'],
                    result['layer_snapshot_exists'],
                    result['signature_valid'],
                    result['status'],
                    result['error_details']
                ])
                f.flush()
        except Exception as e:
            logger.error(f"Failed to write result: {e}")
    
    def validate_all_attestations(self):
        """
        Main validation process - validate all collected attestations
        """
        logger.info("Starting attestation validation process")
        
        # load state
        state = self.load_validation_state()
        
        # load data
        checkpoints = self.load_checkpoint_data()
        all_attestations = self.load_attestation_data()
        
        if not checkpoints:
            logger.error("No checkpoint data available")
            return
        
        if not all_attestations:
            logger.error("No attestation data available")
            return
        
        # filter to only new attestations
        attestations = self.filter_new_attestations(all_attestations, state)
        
        if not attestations:
            logger.info("No new attestations to validate")
            return
        
        logger.info(f"Validating {len(attestations)} new attestations against {len(checkpoints)} checkpoints")
        
        # validate each attestation
        valid_count = 0
        malicious_count = 0
        error_count = 0
        processed_count = 0
        
        for i, attestation in enumerate(attestations):
            logger.info(f"Processing attestation {i+1}/{len(attestations)}: {attestation['tx_hash']}")
            
            result = self.validate_attestation(attestation, checkpoints)
            self.write_result(result)
            
            # track statistics
            if result['status'] == 'VALID':
                valid_count += 1
            elif result['status'] == 'MALICIOUS':
                malicious_count += 1
                logger.warning(f"🚨 MALICIOUS ATTESTATION DETECTED: {attestation['tx_hash']}")
            else:
                error_count += 1
            
            processed_count += 1
            
            # update state after each successful validation
            state["last_tx_hash"] = attestation["tx_hash"]
            state["last_block_number"] = attestation["block_number"]
            state["total_validations"] = state["total_validations"] + 1
            state["last_validation_timestamp"] = datetime.utcnow().isoformat()
            self.save_validation_state(state)
            
            # small delay to avoid overwhelming services
            time.sleep(0.1)
        
        # summary
        logger.info(f"Validation complete:")
        logger.info(f"  📊 Processed: {processed_count} new attestations")
        logger.info(f"  ✅ Valid: {valid_count}")
        logger.info(f"  🚨 Malicious: {malicious_count}")
        logger.info(f"  ❌ Errors: {error_count}")
        logger.info(f"  📈 Total validations: {state['total_validations']}")
        logger.info(f"Results saved to: {self.results_file}")
        logger.info(f"State saved to: {self.state_file}")

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Validate attestations against Layer blockchain data")
    parser.add_argument("--layer-rpc", default=DEFAULT_LAYER_RPC_URL, help="Layer RPC URL")
    parser.add_argument("--chain-id", default="layertest-4", help="Layer chain ID")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        verifier = AttestVerifier(args.layer_rpc, args.chain_id)
        verifier.validate_all_attestations()
        
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        raise

if __name__ == "__main__":
    main() 