#!/usr/bin/env python3
"""
Attest Verifier

This component validates attestations by:
1. Decoding verifyOracleData calldata 
2. Finding matching checkpoint for attestationTimestamp
3. Calculating attestation snapshot
5. Validating signature against validator set
4. Querying Layer to verify snapshot exists

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
from eth_keys import keys
from eth_utils import decode_hex

# configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# import config
from config import config, get_config_manager

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
        
        # get config manager for directory paths
        try:
            config_manager = get_config_manager()
            self.data_dir = config_manager.get_validation_dir()
            self.checkpoint_dir = config_manager.get_layer_checkpoints_dir()
            self.oracle_dir = config_manager.get_oracle_dir()
        except RuntimeError:
            # fallback to legacy paths if in legacy mode
            self.data_dir = f"data/validation"
            self.checkpoint_dir = f"data/layer_checkpoints"
            self.oracle_dir = f"data/oracle"
        
        # create data directory
        os.makedirs(self.data_dir, exist_ok=True)
        
        # output files
        self.state_file = f"{self.data_dir}/{chain_id}_attestation_validation_state.json"
        self.validation_csv_file = f"{self.data_dir}/{chain_id}_attestation_validation_results.csv"
        self.failure_log_file = f"{self.data_dir}/{chain_id}_attestation_validation_failures.log"
        
        # initialize CSV file
        self.init_results_csv()
        
        # test connection to Layer
        self.test_layer_connection()
        
        # discord webhook URL (from environment variable)
        self.discord_webhook_url = os.getenv('DISCORD_WEBHOOK_URL')
        if self.discord_webhook_url:
            logger.info("Discord alerts enabled")
        else:
            logger.info("Discord alerts disabled (DISCORD_WEBHOOK_URL not set)")
    
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
                    'attestation_timestamp',
                    'query_id',
                    'value_hash',
                    'report_timestamp',
                    'aggregate_power',
                    'snapshot',
                    'checkpoint_used',
                    'checkpoint_timestamp',
                    'is_valid_validator_signature',
                    'signing_validator_count',
                    'signing_power_percentage',
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
        checkpoint_file = f"{self.checkpoint_dir}/{self.chain_id}_checkpoints.csv"
        
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
        attestation_file = f"{self.oracle_dir}/attestations.csv"  # note: may need to adjust for multi-chain
        
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

    def load_validator_set_by_checkpoint(self, checkpoint: str) -> Optional[List[Dict[str, Any]]]:
        """
        Load validator set data by checkpoint from validator sets CSV
        """
        valset_file = f"{self.checkpoint_dir}/{self.chain_id}_validator_sets.csv"
        
        if not os.path.exists(valset_file):
            logger.warning(f"Validator set data not found: {valset_file}")
            return None
        
        validator_set = []
        try:
            with open(valset_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row['valset_checkpoint'] == checkpoint:
                        validator_set.append({
                            'ethereumAddress': row['ethereum_address'],
                            'power': int(row['power']),
                            'scrape_timestamp': row['scrape_timestamp'],
                            'valset_timestamp': row['valset_timestamp']
                        })
            
            if validator_set:
                logger.debug(f"Loaded validator set for checkpoint {checkpoint}: {len(validator_set)} validators")
                return validator_set
            else:
                logger.warning(f"No validator set found for checkpoint {checkpoint}")
                return None
                
        except Exception as e:
            logger.error(f"Error loading validator set for checkpoint {checkpoint}: {e}")
            return None

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

    def verify_signature_against_validators(self, snapshot: str, signatures: List[Dict[str, Any]], 
                                          validator_set: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Verify if any signature in the list was signed by a validator in the set
        Returns dict with verification result
        """
        try:
            # sha256 hash the snapshot as mentioned by user
            snapshot_bytes = decode_hex(snapshot)
            message_hash = hashlib.sha256(snapshot_bytes).digest()
            
            # create validator address lookup
            validator_addresses = {v['ethereumAddress'].lower() for v in validator_set}
            total_validator_power = sum(v['power'] for v in validator_set)
            
            verified_signatures = []
            total_signing_power = 0
            
            for i, signature in enumerate(signatures):
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
            logger.error(f"Error verifying signature against validators: {e}")
            return {
                'is_valid_validator': False,
                'verified_signatures': [],
                'total_signing_power': 0,
                'total_validator_power': 0,
                'signing_percentage': 0
            }

    def send_discord_alert(self, alert_type: str, attestation_data: Dict[str, Any], validation_result: Dict[str, Any]):
        """
        Send Discord alert for malicious attestations from valid validators
        """
        if not self.discord_webhook_url:
            logger.debug("Discord webhook URL not configured, skipping alert")
            return
        
        try:
            if alert_type == "malicious_attestation":
                title = "🚨 MALICIOUS ATTESTATION DETECTED"
                color = 0xFF0000  # red
                description = "A malicious attestation was signed by a valid Tellor validator!"
            elif alert_type == "malicious_valset":
                title = "⚠️ MALICIOUS VALIDATOR SET UPDATE"
                color = 0xFF8C00  # orange
                description = "A malicious validator set update was detected!"
            else:
                return
            
            # build embed
            embed = {
                "title": title,
                "description": description,
                "color": color,
                "timestamp": datetime.utcnow().isoformat(),
                "fields": [
                    {
                        "name": "Transaction Hash",
                        "value": f"`{attestation_data.get('tx_hash', 'Unknown')}`",
                        "inline": True
                    },
                    {
                        "name": "Block Number",
                        "value": str(attestation_data.get('block_number', 'Unknown')),
                        "inline": True
                    },
                    {
                        "name": "Query ID",
                        "value": f"`{validation_result.get('query_id', 'Unknown')}`",
                        "inline": False
                    },
                    {
                        "name": "Snapshot",
                        "value": f"`{validation_result.get('snapshot', 'Unknown')[:20]}...`",
                        "inline": False
                    }
                ]
            }
            
            # add validator info if available
            if 'signature_verification' in validation_result:
                sig_result = validation_result['signature_verification']
                verified_sigs = sig_result.get('verified_signatures', [])
                if verified_sigs:
                    validator_info = []
                    for sig in verified_sigs[:3]:  # limit to first 3
                        validator_info.append(f"`{sig['address'][:10]}...` (Power: {sig['power']})")
                    
                    embed["fields"].append({
                        "name": "Signing Validators",
                        "value": "\n".join(validator_info),
                        "inline": False
                    })
                    
                    embed["fields"].append({
                        "name": "Signing Power",
                        "value": f"{sig_result.get('signing_percentage', 0):.1f}% ({sig_result.get('total_signing_power', 0)}/{sig_result.get('total_validator_power', 0)})",
                        "inline": True
                    })
            
            payload = {
                "embeds": [embed]
            }
            
            response = requests.post(self.discord_webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            
            logger.info(f"Discord alert sent for {alert_type}: {attestation_data.get('tx_hash', 'Unknown')}")
            
        except Exception as e:
            logger.error(f"Failed to send Discord alert: {e}")
    
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
                          signatures: List[Dict[str, Any]], snapshot: str) -> Dict[str, Any]:
        """
        Validate signatures against the validator set
        Returns dict with validation results
        """
        if not validator_set or not signatures:
            return {
                'is_valid_validator': False,
                'verified_signatures': [],
                'total_signing_power': 0,
                'total_validator_power': 0,
                'signing_percentage': 0
            }
        
        return self.verify_signature_against_validators(snapshot, signatures, validator_set)
    
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
            'is_valid_validator_signature': False,
            'signing_validator_count': 0,
            'signing_power_percentage': 0.0,
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
            
                        # step 3: load validator set for the checkpoint
            validator_set = self.load_validator_set_by_checkpoint(matching_checkpoint['checkpoint'])
            if not validator_set:
                result['error_details'] = 'Failed to load validator set for checkpoint'
                return result

            # step 4: calculate snapshot
            snapshot = self.calculate_attestation_snapshot(decoded_data, matching_checkpoint['checkpoint'])
            if not snapshot:
                result['error_details'] = 'Failed to calculate snapshot'
                return result

            result['snapshot'] = snapshot
            
            # step 5: verify signatures against Tellor validator set FIRST
            signature_verification = self.validate_signature(
                decoded_data, 
                validator_set,
                decoded_data.get('signatures', []),
                snapshot
            )
            
            result['is_valid_validator_signature'] = signature_verification['is_valid_validator']
            result['signing_validator_count'] = len(signature_verification['verified_signatures'])
            result['signing_power_percentage'] = signature_verification['signing_percentage']
            result['signature_verification'] = signature_verification  # store for Discord alerts
            
            # only proceed with Layer verification if signatures are from valid validators
            if not signature_verification['is_valid_validator']:
                result['status'] = 'INVALID_SIGNATURE'
                result['error_details'] = 'Attestation not signed by valid Tellor validators'
                result['signature_valid'] = False
                logger.debug(f"Skipping Layer verification for {attestation['tx_hash']} - invalid validator signature")
                return result
            
            logger.info(f"Valid validator signature detected: {len(signature_verification['verified_signatures'])} validators, "
                       f"{signature_verification['signing_percentage']:.1f}% power")
            
            # step 6: query Layer (only for attestations signed by valid validators)
            layer_exists = self.query_layer_snapshot(snapshot)
            result['layer_snapshot_exists'] = layer_exists
            result['signature_valid'] = True
            
            if not layer_exists:
                result['status'] = 'MALICIOUS'
                result['error_details'] = 'Snapshot does not exist on Layer - MALICIOUS ATTESTATION FROM VALID VALIDATOR'
                
                # send Discord alert for malicious attestation from valid validator
                logger.warning(f"🚨 MALICIOUS ATTESTATION from valid validators: {attestation['tx_hash']}")
                self.send_discord_alert('malicious_attestation', attestation, result)
                
                return result

            # attestation is valid
            result['status'] = 'VALID'
            
        except Exception as e:
            result['error_details'] = str(e)
            logger.error(f"Error validating attestation {attestation['tx_hash']}: {e}")
        
        return result
    
    def write_result(self, result: Dict[str, Any]):
        """Write validation result to CSV"""
        try:
            with open(self.validation_csv_file, 'a', newline='') as f:
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
                    result['is_valid_validator_signature'],
                    result['signing_validator_count'],
                    result['signing_power_percentage'],
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
        invalid_signature_count = 0
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
            elif result['status'] == 'INVALID_SIGNATURE':
                invalid_signature_count += 1
                logger.debug(f"Invalid signature from non-validator: {attestation['tx_hash']}")
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
        logger.info(f"  🔒 Invalid signatures: {invalid_signature_count}")
        logger.info(f"  ❌ Errors: {error_count}")
        logger.info(f"  📈 Total validations: {state['total_validations']}")
        logger.info(f"Results saved to: {self.validation_csv_file}")
        logger.info(f"State saved to: {self.state_file}")
        
        if malicious_count > 0:
            logger.warning(f"🚨 {malicious_count} malicious attestations detected from valid validators!")
        if invalid_signature_count > 0:
            logger.info(f"🔒 {invalid_signature_count} attestations filtered out (invalid validator signatures)")