#!/usr/bin/env python3
"""
Configuration Manager for Bridge Data Collector

Supports multiple bridge configurations with:
1. Environment variable substitution (${VAR} patterns)
2. Configuration validation
3. Directory-based data organization
4. Backward compatibility
"""

import os
import json
import re
from typing import Dict, Any, Optional
from pathlib import Path

# load environment variables from .env file
try:
    from dotenv import load_dotenv
    # look for .env file in the parent directory (bridge-data-collector root)
    env_path = Path(__file__).parent.parent / '.env'
    load_dotenv(env_path)
except ImportError:
    print("Warning: python-dotenv not installed. Install with: pip install python-dotenv")
except Exception as e:
    print(f"Warning: Could not load .env file: {e}")

class ConfigManager:
    """Advanced configuration manager supporting multiple bridge configurations"""
    
    def __init__(self, config_file: Optional[str] = None, config_name_override: Optional[str] = None):
        self.config_file = config_file or "config.json"
        self._config_data = None
        self._active_config_name = None
        self._active_config = None
        self._config_name_override = config_name_override
        self._load_config()
        self._load_active_config()
    
    def _load_config(self):
        """Load configuration from JSON file"""
        config_path = Path(__file__).parent.parent / self.config_file
        
        try:
            with open(config_path, 'r') as f:
                content = f.read()
                # substitute environment variables
                content = self._substitute_env_vars(content)
                self._config_data = json.loads(content)
        except FileNotFoundError:
            raise FileNotFoundError(f"Config file {config_path} not found")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in config file {config_path}: {e}")
    
    def _substitute_env_vars(self, content: str) -> str:
        """Substitute ${VAR} patterns with environment variables"""
        def replace_var(match):
            var_name = match.group(1)
            env_value = os.getenv(var_name)
            if env_value is None:
                raise ValueError(f"Environment variable {var_name} is not set")
            return env_value
        
        # pattern to match ${VAR_NAME}
        pattern = r'\$\{([A-Z_][A-Z0-9_]*)\}'
        return re.sub(pattern, replace_var, content)
    
    def _load_active_config(self):
        """Load the active configuration based on override, ACTIVE_CONFIG env var, or default"""
        # check for config name override first (from CLI flag)
        if self._config_name_override:
            self._active_config_name = self._config_name_override
        else:
            # fallback to environment variable
            self._active_config_name = os.getenv('ACTIVE_CONFIG')
        
        if not self._active_config_name:
            # try to use the first available config as default
            configs = self.get_available_configs()
            if configs:
                self._active_config_name = list(configs.keys())[0]
                print(f"Warning: ACTIVE_CONFIG not set, using first available config: {self._active_config_name}")
            else:
                raise ValueError("No configurations available and ACTIVE_CONFIG not set")
        
        if self._active_config_name not in self.get_available_configs():
            available = list(self.get_available_configs().keys())
            raise ValueError(f"Active config '{self._active_config_name}' not found. Available: {available}")
        
        self._active_config = self.get_available_configs()[self._active_config_name]
        
        # ensure data directory exists
        data_dir = self.get_data_dir()
        os.makedirs(data_dir, exist_ok=True)
        
        # create subdirectories
        for subdir in ['layer_checkpoints', 'valset', 'oracle', 'validation']:
            os.makedirs(f"{data_dir}/{subdir}", exist_ok=True)
    
    def get_available_configs(self) -> Dict[str, Any]:
        """Get all available configurations"""
        return self._config_data.get("configs", {})
    
    def get_active_config_name(self) -> str:
        """Get the name of the active configuration"""
        return self._active_config_name
    
    def get_active_config(self) -> Dict[str, Any]:
        """Get the active configuration"""
        return self._active_config.copy()
    
    def list_configs(self) -> Dict[str, str]:
        """List all available configurations with display names"""
        configs = {}
        for name, config in self.get_available_configs().items():
            display_name = config.get('display_name', name)
            configs[name] = display_name
        return configs
    
    def switch_config(self, config_name: str):
        """Switch to a different configuration"""
        if config_name not in self.get_available_configs():
            available = list(self.get_available_configs().keys())
            raise ValueError(f"Config '{config_name}' not found. Available: {available}")
        
        # update environment variable (for current session only)
        os.environ['ACTIVE_CONFIG'] = config_name
        
        # reload active config
        self._load_active_config()
        
        print(f"Switched to configuration: {config_name}")
        return True
    
    def validate_config(self, config_name: Optional[str] = None) -> Dict[str, Any]:
        """Validate a configuration and return validation results"""
        config = self.get_available_configs().get(config_name or self._active_config_name)
        
        if not config:
            return {"valid": False, "errors": ["Configuration not found"]}
        
        errors = []
        warnings = []
        
        # required fields (allow RPCs to be provided as single or list)
        required_fields_base = [
            'layer_chain', 'evm_chain', 'bridge_contract', 'data_dir', 'database_path'
        ]
        
        for field in required_fields_base:
            if field not in config:
                errors.append(f"Missing required field: {field}")
        
        # validate RPC URLs (string or list of strings)
        def _is_http_url(s: str) -> bool:
            return isinstance(s, str) and s.startswith(('http://', 'https://'))

        # Layer RPC: either layer_rpc_urls (list) or layer_rpc_url (string) must be present
        layer_urls = config.get('layer_rpc_urls')
        layer_url = config.get('layer_rpc_url')
        if layer_urls is not None:
            if not isinstance(layer_urls, list) or not layer_urls or not all(_is_http_url(u) for u in layer_urls):
                errors.append("layer_rpc_urls must be a non-empty list of HTTP/HTTPS URLs")
        elif layer_url is not None:
            if not _is_http_url(layer_url):
                errors.append("layer_rpc_url must be a valid HTTP/HTTPS URL")
        else:
            errors.append("Missing required field: layer_rpc_url or layer_rpc_urls")

        # EVM RPC: either evm_rpc_urls (list) or evm_rpc_url (string) must be present
        evm_urls = config.get('evm_rpc_urls')
        evm_url = config.get('evm_rpc_url')
        if evm_urls is not None:
            if not isinstance(evm_urls, list) or not evm_urls or not all(_is_http_url(u) for u in evm_urls):
                errors.append("evm_rpc_urls must be a non-empty list of HTTP/HTTPS URLs")
        elif evm_url is not None:
            if not _is_http_url(evm_url):
                errors.append("evm_rpc_url must be a valid HTTP/HTTPS URL")
        else:
            errors.append("Missing required field: evm_rpc_url or evm_rpc_urls")
        
        # validate bridge contract address
        if 'bridge_contract' in config:
            contract = config['bridge_contract']
            if not contract.startswith('0x') or len(contract) != 42:
                errors.append("bridge_contract must be a valid Ethereum address (0x...)")
        
        # validate databank contract address (optional)
        if 'databank_contract' in config and config['databank_contract']:
            contract = config['databank_contract']
            if not contract.startswith('0x') or len(contract) != 42:
                errors.append("databank_contract must be a valid Ethereum address (0x...)")
        
        # validate optional fields
        if 'discord_webhook_url' in config:
            webhook = config['discord_webhook_url']
            if webhook and not webhook.startswith('https://discord.com/api/webhooks/'):
                warnings.append("discord_webhook_url should be a Discord webhook URL")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "config_name": config_name or self._active_config_name
        }
    
    # configuration getters using active config
    
    def get_layer_chain(self) -> str:
        """Get layer chain ID"""
        return self._active_config['layer_chain']
    
    def get_evm_chain(self) -> str:
        """Get EVM chain name"""
        return self._active_config['evm_chain']
    
    def get_bridge_contract(self) -> str:
        """Get bridge contract address"""
        return self._active_config['bridge_contract']
    
    def get_databank_contract(self) -> Optional[str]:
        """Get TellorDataBank contract address"""
        return self._active_config.get('databank_contract')
    
    def get_layer_rpc_url(self) -> str:
        """Get Layer RPC URL (primary)"""
        urls = self._active_config.get('layer_rpc_urls')
        if isinstance(urls, list) and urls:
            return urls[0]
        return self._active_config['layer_rpc_url']
    
    def get_evm_rpc_url(self) -> str:
        """Get EVM RPC URL (primary)"""
        urls = self._active_config.get('evm_rpc_urls')
        if isinstance(urls, list) and urls:
            return urls[0]
        return self._active_config['evm_rpc_url']

    def get_layer_rpc_urls(self) -> list:
        """Get list of Layer RPC URLs in preference order"""
        urls = self._active_config.get('layer_rpc_urls')
        if isinstance(urls, list) and urls:
            return urls
        url = self._active_config.get('layer_rpc_url')
        if isinstance(url, str) and url:
            return [url]
        raise ValueError("No Layer RPC URL(s) configured")

    def get_evm_rpc_urls(self) -> list:
        """Get list of EVM RPC URLs in preference order"""
        urls = self._active_config.get('evm_rpc_urls')
        if isinstance(urls, list) and urls:
            return urls
        url = self._active_config.get('evm_rpc_url')
        if isinstance(url, str) and url:
            return [url]
        raise ValueError("No EVM RPC URL(s) configured")

    def get_rpc_preference_reset_minutes(self) -> int:
        """Get preference reset interval (minutes) for RPC selection (default 60)"""
        try:
            return int(self._active_config.get('rpc_preference_reset_minutes', 60))
        except Exception:
            return 60
    
    def get_discord_webhook_url(self) -> Optional[str]:
        """Get Discord webhook URL"""
        return self._active_config.get('discord_webhook_url')
    
    def get_discord_webhook(self, webhook_type: str) -> Optional[str]:
        """Get specific Discord webhook URL by type"""
        # try new discord_webhooks structure first
        webhooks = self._active_config.get('discord_webhooks', {})
        if webhook_type in webhooks:
            return webhooks[webhook_type]
        
        # fallback to legacy webhook for malicious_activity
        if webhook_type == 'malicious_activity':
            return self.get_discord_webhook_url()
        
        return None
    
    def get_data_dir(self) -> str:
        """Get data directory for active configuration"""
        return self._active_config['data_dir']
    
    def get_display_name(self) -> str:
        """Get display name for active configuration"""
        return self._active_config.get('display_name', self._active_config_name)
    
    def get_database_path(self) -> str:
        """Get database path for active configuration"""
        return self._active_config.get('database_path', f"databases/{self._active_config_name}.duckdb")
    
    def create_database_manager(self):
        """Create a database manager instance for the active configuration"""
        from database_manager import BridgeMonitorDB
        
        db = BridgeMonitorDB(
            config_name=self._active_config_name,
            database_path=self.get_database_path()
        )
        
        # store configuration metadata in the database
        db.set_config_metadata(
            display_name=self.get_display_name(),
            layer_chain=self.get_layer_chain(),
            evm_chain=self.get_evm_chain(),
            bridge_contract=self.get_bridge_contract(),
            layer_rpc_url=self.get_layer_rpc_url(),
            evm_rpc_url=self.get_evm_rpc_url()
        )
        
        return db
    
    # path helpers for data files
    
    def get_layer_checkpoints_dir(self) -> str:
        """Get layer checkpoints directory"""
        return f"{self.get_data_dir()}/layer_checkpoints"
    
    def get_valset_dir(self) -> str:
        """Get validator set events directory"""
        return f"{self.get_data_dir()}/valset"
    
    def get_oracle_dir(self) -> str:
        """Get oracle/attestation directory"""
        return f"{self.get_data_dir()}/oracle"
    
    def get_validation_dir(self) -> str:
        """Get validation results directory"""
        return f"{self.get_data_dir()}/validation"
    
    def get_state_file(self, component: str) -> str:
        """Get state file path for a component"""
        if component == "bridge_watcher":
            return f"{self.get_data_dir()}/bridge_watcher_state.json"
        elif component == "checkpoint_scribe":
            return f"{self.get_layer_checkpoints_dir()}/{self.get_layer_chain()}_checkpoints_state.json"
        elif component == "valset_watcher":
            return f"{self.get_valset_dir()}/valset_updates_state.json"
        elif component == "attest_watcher":
            return f"{self.get_oracle_dir()}/attestations_state.json"
        elif component == "valset_verifier":
            return f"{self.get_validation_dir()}/{self.get_layer_chain()}_valset_validation_state.json"
        elif component == "attest_verifier":
            return f"{self.get_validation_dir()}/{self.get_layer_chain()}_attestation_validation_state.json"
        else:
            raise ValueError(f"Unknown component: {component}")
    
    # defaults and monitoring config
    
    def get_defaults(self) -> Dict[str, Any]:
        """Get default values"""
        return self._config_data.get("defaults", {})
    
    def get_monitoring_config(self) -> Dict[str, Any]:
        """Get monitoring configuration"""
        return self._config_data.get("monitoring", {})
    
    def get_contract_config(self) -> Dict[str, Any]:
        """Get contract configuration (selectors, addresses, etc.)"""
        return self._config_data.get("contracts", {})
    
    # monitoring configuration getters (backward compatibility)
    
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


# Global configuration manager instance
_config_manager_instance = None

def get_config_manager() -> ConfigManager:
    """
    Get the global configuration manager instance (singleton pattern)
    """
    global _config_manager_instance
    
    if _config_manager_instance is None:
        _config_manager_instance = ConfigManager()
    
    return _config_manager_instance

def reset_config_manager_instance(config_name_override: Optional[str] = None):
    """
    Reset the global configuration manager instance with optional config override
    """
    global _config_manager_instance
    _config_manager_instance = ConfigManager(config_name_override=config_name_override)