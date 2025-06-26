#!/usr/bin/env python3
"""
Configuration management for Bridge Data Collector

Loads configuration from:
1. Environment variables (for secrets)
2. config.json file (for non-secrets)  
3. Defaults (as fallbacks)
"""

import os
import json
from typing import Dict, Any, Optional
from pathlib import Path

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    # Look for .env file in the parent directory (bridge-data-collector root)
    env_path = Path(__file__).parent.parent / '.env'
    load_dotenv(env_path)
except ImportError:
    print("Warning: python-dotenv not installed. Install with: pip install python-dotenv")
except Exception as e:
    print(f"Warning: Could not load .env file: {e}")

class Config:
    """Configuration manager that loads from env vars and config file"""
    
    def __init__(self, config_file: Optional[str] = None):
        self.config_file = config_file or "config.json"
        self._config_data = None
        self._load_config()
    
    def _load_config(self):
        """Load configuration from JSON file"""
        config_path = Path(__file__).parent.parent / self.config_file
        
        try:
            with open(config_path, 'r') as f:
                self._config_data = json.load(f)
        except FileNotFoundError:
            print(f"Warning: Config file {config_path} not found, using environment variables only")
            self._config_data = {}
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in config file {config_path}: {e}")
    
    def get_network_config(self, network: str = "sepolia") -> Dict[str, Any]:
        """Get network-specific configuration"""
        networks = self._config_data.get("networks", {})
        return networks.get(network, {})
    
    def get_contract_config(self) -> Dict[str, Any]:
        """Get contract configuration (selectors, addresses, etc.)"""
        return self._config_data.get("contracts", {})
    
    def get_monitoring_config(self) -> Dict[str, Any]:
        """Get monitoring configuration"""
        return self._config_data.get("monitoring", {})
    
    def get_defaults(self) -> Dict[str, Any]:
        """Get default values"""
        return self._config_data.get("defaults", {})
    
    # Main configuration getters with environment variable override
    
    def get_evm_rpc_url(self) -> str:
        """Get EVM RPC URL (from env var, no default for security)"""
        url = os.getenv('EVM_RPC_URL')
        if not url:
            raise ValueError(
                "EVM_RPC_URL environment variable is required! "
                "Copy .env.example to .env and set your API key."
            )
        return url
    
    def get_layer_rpc_url(self) -> str:
        """Get Layer RPC URL"""
        return os.getenv(
            'LAYER_RPC_URL', 
            self.get_network_config().get('layer_rpc_url', 'https://node-palmito.tellorlayer.com')
        )
    
    def get_bridge_address(self) -> str:
        """Get bridge contract address"""
        return os.getenv(
            'BRIDGE_ADDRESS',
            self.get_network_config().get('bridge_address', '0xC69f43741D379cE93bdaAC9b5135EA3e697df1F8')
        )
    
    def get_chain_id(self) -> str:
        """Get chain ID"""
        return os.getenv(
            'CHAIN_ID',
            self.get_network_config().get('chain_id', 'layertest-4')
        )
    
    # Contract configuration getters
    
    def get_function_selector(self, function_name: str) -> str:
        """Get function selector by name"""
        selectors = self.get_contract_config().get('function_selectors', {})
        if function_name not in selectors:
            raise ValueError(f"Function selector for '{function_name}' not found in config")
        return selectors[function_name]
    
    def get_domain_separator(self, separator_name: str) -> str:
        """Get domain separator by name"""
        separators = self.get_contract_config().get('domain_separators', {})
        if separator_name not in separators:
            raise ValueError(f"Domain separator for '{separator_name}' not found in config")
        return separators[separator_name]
    
    # Monitoring configuration getters
    
    def get_block_batch_size(self) -> int:
        """Get block batch size for monitoring"""
        return self.get_monitoring_config().get('block_batch_size', 2000)
    
    def get_max_retries(self) -> int:
        """Get maximum number of retries"""
        return self.get_monitoring_config().get('max_retries', 3)
    
    def get_retry_delay(self) -> int:
        """Get retry delay in seconds"""
        return self.get_monitoring_config().get('retry_delay', 5)
    
    def get_catchup_delay(self) -> int:
        """Get catchup delay in seconds"""
        return self.get_monitoring_config().get('catchup_delay', 2)
    
    def get_monitoring_interval(self) -> int:
        """Get default monitoring interval"""
        return self.get_defaults().get('monitoring_interval', 300)
    
    def get_block_buffer(self) -> int:
        """Get block buffer for monitoring"""
        return self.get_defaults().get('block_buffer', 5)

# Global config instance
config = Config()

# Convenience functions for backward compatibility
def get_evm_rpc_url() -> str:
    return config.get_evm_rpc_url()

def get_layer_rpc_url() -> str:
    return config.get_layer_rpc_url()

def get_bridge_address() -> str:
    return config.get_bridge_address()

def get_chain_id() -> str:
    return config.get_chain_id()

def get_function_selector(function_name: str) -> str:
    return config.get_function_selector(function_name)

def get_domain_separator(separator_name: str) -> str:
    return config.get_domain_separator(separator_name) 