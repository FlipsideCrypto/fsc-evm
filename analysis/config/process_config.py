#!/usr/bin/env python
"""
Configuration Processor for FSC-EVM

This script processes the CSV configuration files and generates
a flattened JSON configuration for each chain.

It handles template expressions in the form {{ variable_name * 2 }}
by evaluating them against other variables.
"""

import os
import csv
import json
import re
import logging
from typing import Dict, Any, List, Optional

# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('config_processor')

class ConfigProcessor:
    def __init__(self, fsc_evm_dir: str = None):
        """Initialize the configuration processor.
        
        Args:
            fsc_evm_dir: Path to the FSC-EVM repository. If not provided,
                         attempts to detect automatically.
        """
        self.fsc_evm_dir = fsc_evm_dir or self._find_fsc_evm_dir()
        self.keys_file = os.path.join(self.fsc_evm_dir, 'data', 'bronze_keys.csv')
        self.values_file = os.path.join(self.fsc_evm_dir, 'data', 'bronze_values.csv')
        self.output_dir = os.path.join(self.fsc_evm_dir, 'logs', 'config_cache')
        
        # Will hold configuration by chain
        self.chains = {}
        self.chain_configs = {}
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
    
    def _find_fsc_evm_dir(self) -> str:
        """Find the FSC-EVM repository directory."""
        # Start with current directory
        current_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Go up two levels (from analysis/config)
        fsc_evm_dir = os.path.dirname(os.path.dirname(current_dir))
        
        # Verify we found the right directory by checking for dbt_project.yml
        if os.path.exists(os.path.join(fsc_evm_dir, 'dbt_project.yml')):
            return fsc_evm_dir
        
        # If not found, raise error
        raise ValueError("Could not locate FSC-EVM repository root directory")
    
    def load_keys(self) -> Dict[str, Dict[str, Any]]:
        """Load the configuration schema from bronze_keys.csv."""
        logger.info(f"Loading keys from {self.keys_file}")
        
        keys = {}
        with open(self.keys_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = row.get('key', '')
                if not key:
                    continue
                
                # Convert default value to appropriate type
                default_value = self._convert_value(
                    row.get('default_value', ''), 
                    row.get('data_type', 'STRING')
                )
                
                # Store key information
                keys[key] = {
                    'parent_key': row.get('parent_key') if row.get('parent_key') != 'NULL' else None,
                    'data_type': row.get('data_type', 'STRING'),
                    'package': row.get('package', ''),
                    'category': row.get('category') if row.get('category') != 'NULL' else None,
                    'default_value': default_value,
                    'value': default_value,  # Start with default value
                    'is_enabled': True  # Default to enabled
                }
        
        logger.info(f"Loaded {len(keys)} keys")
        return keys
    
    def load_values(self) -> None:
        """Load chain-specific values from bronze_values.csv."""
        logger.info(f"Loading values from {self.values_file}")
        
        with open(self.values_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                chain = row.get('chain', '')
                if not chain:
                    continue
                
                # Initialize chain config if not already done
                if chain not in self.chains:
                    self.chains[chain] = True
                    self.chain_configs[chain] = self.load_keys()  # Start with defaults
                
                key = row.get('key', '')
                if not key:
                    continue
                
                # Skip if key not in schema
                if key not in self.chain_configs[chain]:
                    logger.warning(f"Key '{key}' not defined in schema, skipping")
                    continue
                
                # Get config entry
                config = self.chain_configs[chain][key]
                
                # Get parent key
                parent_key = row.get('parent_key')
                if parent_key == 'NULL':
                    parent_key = None
                
                # Get enabled status
                is_enabled_str = row.get('is_enabled', 'TRUE')
                is_enabled = is_enabled_str.upper() in ('TRUE', 'YES', '1')
                
                # Get value with appropriate type
                typed_value = self._convert_value(
                    row.get('value', ''), 
                    config['data_type']
                )
                
                # Update configuration
                config['value'] = typed_value
                config['parent_key'] = parent_key
                config['is_enabled'] = is_enabled
        
        logger.info(f"Loaded values for {len(self.chains)} chains")
    
    def _convert_value(self, value: str, data_type: str) -> Any:
        """Convert a string value to the appropriate type."""
        if value is None or value == '':
            return None
            
        data_type = data_type.upper()
        
        if data_type == 'BOOLEAN':
            return value.upper() in ('TRUE', 'YES', '1')
        elif data_type == 'NUMBER':
            try:
                if '.' in value:
                    return float(value)
                else:
                    return int(value)
            except ValueError:
                logger.warning(f"Could not convert '{value}' to NUMBER, using as STRING")
                return value
        else:  # Default to STRING
            return value
    
    def resolve_templates(self) -> None:
        """Resolve template expressions in all chain configurations."""
        logger.info("Resolving template expressions")
        
        for chain, config in self.chain_configs.items():
            logger.info(f"Processing chain: {chain}")
            
            # Create a simplified version of the config with just values
            values_dict = {k: v['value'] for k, v in config.items()}
            
            # First pass: identify templates
            templates = []
            for key, entry in config.items():
                if isinstance(entry['value'], str) and '{{' in entry['value'] and '}}' in entry['value']:
                    templates.append(key)
            
            # Process templates until no more can be resolved
            iterations = 0
            while templates and iterations < 10:  # Prevent infinite loops
                iterations += 1
                still_templated = []
                
                for key in templates:
                    entry = config[key]
                    template = entry['value']
                    
                    try:
                        # Extract the expression between {{ and }}
                        match = re.search(r'{{(.*)}}', template)
                        if match:
                            expression = match.group(1).strip()
                            
                            # Create evaluation context with current values
                            result = eval(expression, {"__builtins__": {}}, values_dict)
                            
                            # Update the configuration
                            entry['value'] = result
                            values_dict[key] = result
                            logger.info(f"Resolved template for {key}: {expression} => {result}")
                        else:
                            still_templated.append(key)
                            logger.warning(f"Could not extract expression from template: {template}")
                    except Exception as e:
                        still_templated.append(key)
                        logger.error(f"Failed to resolve template for {key}: {template}. Error: {e}")
                
                # Update template list for next iteration
                if len(still_templated) == len(templates):
                    # No progress made, break out
                    logger.warning(f"Could not resolve {len(still_templated)} templates after {iterations} iterations")
                    break
                    
                templates = still_templated
            
            logger.info(f"Resolved templates for chain {chain} in {iterations} iterations")
    
    def build_hierarchical_configs(self) -> Dict[str, Dict[str, Any]]:
        """Build hierarchical configurations for all chains."""
        logger.info("Building hierarchical configurations")
        
        hierarchical_configs = {}
        
        for chain, config in self.chain_configs.items():
            hierarchical = {}
            
            # First pass: create entries for all keys
            for key, entry in config.items():
                if not entry['is_enabled']:
                    continue
                    
                hierarchical[key] = {
                    'value': entry['value'],
                    'children': {}
                }
            
            # Second pass: add children to their parents
            for key, entry in config.items():
                if not entry['is_enabled']:
                    continue
                    
                parent_key = entry['parent_key']
                if parent_key and parent_key in hierarchical:
                    hierarchical[parent_key]['children'][key] = {
                        'value': entry['value']
                    }
            
            hierarchical_configs[chain] = hierarchical
        
        return hierarchical_configs
    
    def write_configs(self, hierarchical_configs: Dict[str, Dict[str, Any]]) -> None:
        """Write configurations to JSON files."""
        logger.info(f"Writing configurations to {self.output_dir}")
        
        for chain, config in self.chain_configs.items():
            # Prepare flat version (just key:value pairs)
            flat_config = {k: v['value'] for k, v in config.items() if v['is_enabled']}
            
            # Write flat config
            flat_path = os.path.join(self.output_dir, f'config_chain_{chain}.json')
            with open(flat_path, 'w') as f:
                json.dump(flat_config, f, indent=2)
            
            # Write hierarchical config
            hier_path = os.path.join(self.output_dir, f'config_hierarchical_{chain}.json')
            with open(hier_path, 'w') as f:
                json.dump(hierarchical_configs[chain], f, indent=2)
            
            logger.info(f"Wrote configurations for chain {chain}")
            
        # Write a list of all chains
        chains_path = os.path.join(self.output_dir, 'chains.json')
        with open(chains_path, 'w') as f:
            json.dump(list(self.chains.keys()), f, indent=2)
    
    def process(self) -> None:
        """Process configurations for all chains."""
        # Load keys and values
        self.load_values()
        
        # Resolve template expressions
        self.resolve_templates()
        
        # Build hierarchical configurations
        hierarchical_configs = self.build_hierarchical_configs()
        
        # Write configurations to files
        self.write_configs(hierarchical_configs)
        
        logger.info("Configuration processing complete")

if __name__ == "__main__":
    processor = ConfigProcessor()
    processor.process()