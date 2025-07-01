#!/usr/bin/env python3
"""
Configuration management for Bridge Data Collector

Updated to use ConfigManager for multi-config support while maintaining
backward compatibility with existing component interfaces.

Loads configuration from:
1. Environment variables (for secrets and active config selection)
2. config.json file (for multi-config structure)  
3. Defaults (as fallbacks)
"""

import os
import json
from typing import Dict, Any, Optional
from pathlib import Path

# import the new configuration manager
from config_manager import ConfigManager

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
    """
    Backward-compatible configuration wrapper using ConfigManager
    """
    
    def __init__(self, config_file: Optional[str] = None):
        try:
            self.config_manager = ConfigManager(config_file)
        except Exception as e:
            # fallback to old behavior if new config fails
            print(f"Warning: Could not load new config format: {e}")
            print("Falling back to legacy configuration...")
            self._legacy_mode = True
            self._load_legacy_config()
            return
        
        self._legacy_mode = False
    
    def _load_legacy_config(self):
        """Fallback legacy config loading"""
        self.config_file = "config.json"
        self._config_data = None
        
        # try to load config file
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
        """Get network-specific configuration (legacy)"""
        if self._legacy_mode:
            networks = self._config_data.get("networks", {})
            return networks.get(network, {})
        else:
            # map to new config structure
            active_config = self.config_manager.get_active_config()
            if active_config.get('evm_chain') == network:
                return {
                    'chain_id': active_config.get('layer_chain'),
                    'bridge_address': active_config.get('bridge_contract'),
                    'layer_rpc_url': active_config.get('layer_rpc_url')
                }
            return {}
    
    def get_contract_config(self) -> Dict[str, Any]:
        """Get contract configuration (selectors, addresses, etc.)"""
        if self._legacy_mode:
            return self._config_data.get("contracts", {})
        else:
            return self.config_manager.get_contract_config()
    
    def get_monitoring_config(self) -> Dict[str, Any]:
        """Get monitoring configuration"""
        if self._legacy_mode:
            return self._config_data.get("monitoring", {})
        else:
            return self.config_manager.get_monitoring_config()
    
    def get_defaults(self) -> Dict[str, Any]:
        """Get default values"""
        if self._legacy_mode:
            return self._config_data.get("defaults", {})
        else:
            return self.config_manager.get_defaults()
    
    # Main configuration getters with environment variable override
    
    def get_evm_rpc_url(self) -> str:
        """Get EVM RPC URL (from env var or config)"""
        if self._legacy_mode:
            url = os.getenv('EVM_RPC_URL')
            if not url:
                raise ValueError(
                    "EVM_RPC_URL environment variable is required! "
                    "Copy .env.example to .env and set your API key."
                )
            return url
        else:
            return self.config_manager.get_evm_rpc_url()
    
    def get_layer_rpc_url(self) -> str:
        """Get Layer RPC URL"""
        if self._legacy_mode:
            return os.getenv(
                'LAYER_RPC_URL', 
                self.get_network_config().get('layer_rpc_url', 'https://node-palmito.tellorlayer.com')
            )
        else:
            return self.config_manager.get_layer_rpc_url()
    
    def get_bridge_address(self) -> str:
        """Get bridge contract address"""
        if self._legacy_mode:
            return os.getenv(
                'BRIDGE_ADDRESS',
                self.get_network_config().get('bridge_address', '0xC69f43741D379cE93bdaAC9b5135EA3e697df1F8')
            )
        else:
            return self.config_manager.get_bridge_contract()
    
    def get_chain_id(self) -> str:
        """Get chain ID"""
        if self._legacy_mode:
            return os.getenv(
                'CHAIN_ID',
                self.get_network_config().get('chain_id', 'layertest-4')
            )
        else:
            return self.config_manager.get_layer_chain()
    
    # Contract configuration getters
    
    def get_function_selector(self, function_name: str) -> str:
        """Get function selector by name"""
        if self._legacy_mode:
            selectors = self.get_contract_config().get('function_selectors', {})
            if function_name not in selectors:
                raise ValueError(f"Function selector for '{function_name}' not found in config")
            return selectors[function_name]
        else:
            return self.config_manager.get_function_selector(function_name)
    
    def get_domain_separator(self, separator_name: str) -> str:
        """Get domain separator by name"""
        if self._legacy_mode:
            separators = self.get_contract_config().get('domain_separators', {})
            if separator_name not in separators:
                raise ValueError(f"Domain separator for '{separator_name}' not found in config")
            return separators[separator_name]
        else:
            return self.config_manager.get_domain_separator(separator_name)
    
    # Monitoring configuration getters
    
    def get_block_batch_size(self) -> int:
        """Get block batch size for monitoring"""
        if self._legacy_mode:
            return self.get_monitoring_config().get('block_batch_size', 2000)
        else:
            return self.config_manager.get_block_batch_size()
    
    def get_max_retries(self) -> int:
        """Get maximum number of retries"""
        if self._legacy_mode:
            return self.get_monitoring_config().get('max_retries', 3)
        else:
            return self.config_manager.get_max_retries()
    
    def get_retry_delay(self) -> int:
        """Get retry delay in seconds"""
        if self._legacy_mode:
            return self.get_monitoring_config().get('retry_delay', 5)
        else:
            return self.config_manager.get_retry_delay()
    
    def get_catchup_delay(self) -> int:
        """Get catchup delay in seconds"""
        if self._legacy_mode:
            return self.get_monitoring_config().get('catchup_delay', 2)
        else:
            return self.config_manager.get_catchup_delay()
    
    def get_monitoring_interval(self) -> int:
        """Get default monitoring interval"""
        if self._legacy_mode:
            return self.get_defaults().get('monitoring_interval', 300)
        else:
            return self.config_manager.get_monitoring_interval()
    
    def get_block_buffer(self) -> int:
        """Get block buffer for monitoring"""
        if self._legacy_mode:
            return self.get_defaults().get('block_buffer', 5)
        else:
            return self.config_manager.get_block_buffer()

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

# New functions for multi-config support
def get_config_manager() -> ConfigManager:
    """Get the underlying config manager for advanced operations"""
    if config._legacy_mode:
        raise RuntimeError("Multi-config features not available in legacy mode")
    return config.config_manager 