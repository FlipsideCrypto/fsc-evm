import os
import yaml
import csv
import json
import logging
import re
from functools import lru_cache

logger = logging.getLogger('dbt')

class ConfigManager:
    """
    Central configuration manager that handles loading and managing 
    configuration for EVM chain-specific projects.
    
    This class:
    1. Detects which project is running
    2. Maps the project to a specific chain ID and chain name
    3. Loads base config schema from bronze_keys.csv
    4. Applies chain-specific values from bronze_values.csv
    5. Resolves template expressions and hierarchical references
    6. Caches the configuration for DBT to use
    """
    
    def __init__(self, project_name=None):
        """
        Initialize the configuration manager.
        
        Args:
            project_name (str, optional): The name of the project. 
                                         If not provided, attempts to detect automatically.
        """
        self.project_name = project_name or self._detect_project_name()
        logger.info(f"Initializing configuration for project: {self.project_name}")
        
        # Get chain information
        self.chain_info = self._get_chain_info_from_project()
        self.chain_id = self.chain_info.get('chain_id')
        self.chain_name = self.chain_info.get('chain_name')
        logger.info(f"Mapped project to chain: {self.chain_name} (ID: {self.chain_id})")
        
        # Will hold all configuration values
        self.config = {}
        
        # Will hold related configurations (children under parents)
        self.hierarchical_config = {}
        
        # Load configurations
        self._load_config()
    
    def _detect_project_name(self):
        """
        Detect the currently running project name from environment.
        
        Returns:
            str: The name of the project
        """
        # First try from environment variable (set by DBT macro)
        project_name = os.environ.get('DBT_PROJECT', '')
        
        if project_name:
            return project_name
            
        # Fallback: Try to detect from current working directory
        cwd = os.getcwd()
        project_dir = os.path.basename(cwd)
        
        if project_dir.endswith('-models'):
            return project_dir
            
        # Last resort: raise error
        raise ValueError("Could not determine project name. Please provide explicitly.")
    
    def _get_chain_info_from_project(self):
        """
        Map project name to chain information using centralized mapping.
        
        Returns:
            dict: Chain information including ID and name
        
        Raises:
            ValueError: If no mapping exists for the project
        """
        # Path is relative to this file
        base_dir = os.path.dirname(os.path.abspath(__file__))
        project_map_path = os.path.join(base_dir, 'project_map.yaml')
        
        with open(project_map_path, 'r') as f:
            project_map = yaml.safe_load(f)
        
        chain_info = project_map.get(self.project_name)
        if not chain_info:
            raise ValueError(f"No chain mapping found for project: {self.project_name}")
        
        return chain_info
    
    @lru_cache(maxsize=1)  # Ensures config is loaded only once per instance
    def _load_config(self):
        """
        Load the full configuration, applying all layers.
        This method is cached to ensure configuration is loaded only once.
        """
        logger.info("Loading configuration layers...")
        
        # 1. Load configuration schema (keys)
        self._load_config_schema()
        logger.info("Loaded configuration schema")
        
        # 2. Apply chain-specific values
        self._apply_chain_values()
        logger.info("Applied chain-specific values")
        
        # 3. Resolve template expressions
        self._resolve_template_expressions()
        logger.info("Resolved template expressions")
        
        # 4. Build hierarchical config
        self._build_hierarchical_config()
        logger.info("Built hierarchical configuration")
        
        # 5. Cache to file for DBT to read
        self._cache_config()
        logger.info("Configuration cached successfully")
    
    def _load_config_schema(self):
        """
        Load the configuration schema from bronze_keys.csv.
        This defines all possible keys and their default values.
        """
        # Find the FSC-EVM directory first
        fsc_evm_dir = self._find_fsc_evm_dir()
        # Keys are in the data directory
        keys_path = os.path.join(fsc_evm_dir, 'data', 'bronze_keys.csv')
        
        if not os.path.exists(keys_path):
            logger.warning(f"No keys file found at {keys_path}, using empty schema")
            return
        
        with open(keys_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = row.get('key', '')
                if not key:
                    continue
                
                # Get typed default value
                default_value = self._convert_value_to_type(
                    row.get('default_value', ''), 
                    row.get('data_type', 'STRING')
                )
                
                # Store in config dictionary
                self.config[key] = {
                    'value': default_value,
                    'parent_key': row.get('parent_key') if row.get('parent_key') != 'NULL' else None,
                    'data_type': row.get('data_type', 'STRING'),
                    'package': row.get('package', ''),
                    'category': row.get('category') if row.get('category') != 'NULL' else None,
                    'is_enabled': True  # Default to enabled
                }
    
    def _apply_chain_values(self):
        """
        Apply chain-specific values from bronze_values.csv.
        This overrides default values with chain-specific ones.
        """
        # Find the FSC-EVM directory first
        fsc_evm_dir = self._find_fsc_evm_dir()
        # Values are in the data directory
        values_path = os.path.join(fsc_evm_dir, 'data', 'bronze_values.csv')
        
        if not os.path.exists(values_path):
            logger.warning(f"No values file found at {values_path}, using defaults only")
            return
        
        with open(values_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                chain = row.get('chain', '')
                
                # Skip if not for current chain or global
                if chain != self.chain_name and chain != 'fsc_evm':
                    continue
                
                key = row.get('key', '')
                if not key:
                    continue
                
                parent_key = row.get('parent_key')
                if parent_key == 'NULL':
                    parent_key = None
                
                is_enabled_str = row.get('is_enabled', 'TRUE')
                is_enabled = is_enabled_str.upper() in ('TRUE', 'YES', '1')
                
                # If key exists, update its value
                if key in self.config:
                    # Get the data type from the schema
                    data_type = self.config[key]['data_type']
                    
                    # Convert value to appropriate type
                    typed_value = self._convert_value_to_type(row.get('value', ''), data_type)
                    
                    # Update the configuration
                    self.config[key]['value'] = typed_value
                    self.config[key]['parent_key'] = parent_key
                    self.config[key]['is_enabled'] = is_enabled
                else:
                    # Key wasn't in schema, assume STRING type
                    logger.warning(f"Key '{key}' not found in schema, adding as STRING")
                    self.config[key] = {
                        'value': row.get('value', ''),
                        'parent_key': parent_key,
                        'data_type': 'STRING',
                        'package': '',
                        'category': None,
                        'is_enabled': is_enabled
                    }
    
    def _convert_value_to_type(self, value, data_type):
        """
        Convert a string value to the appropriate type.
        
        Args:
            value (str): The string value to convert
            data_type (str): The target data type (BOOLEAN, NUMBER, STRING)
            
        Returns:
            The converted value of appropriate type
        """
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
    
    def _resolve_template_expressions(self):
        """
        Resolve template expressions in configuration values.
        
        This handles expressions like:
        {{MAIN_SL_BLOCKS_PER_HOUR * 2}}
        """
        # First, create a simplified version of the config with just values
        values_dict = {k: v['value'] for k, v in self.config.items()}
        
        # Then resolve template expressions
        for key, config in self.config.items():
            if isinstance(config['value'], str) and '{{' in config['value'] and '}}' in config['value']:
                template = config['value']
                
                # Extract the expression
                expression = template.strip()
                if expression.startswith('{{') and expression.endswith('}}'):
                    expression = expression[2:-2].strip()
                    
                    try:
                        # Create a safe evaluation context with only the config values
                        result = eval(expression, {"__builtins__": {}}, values_dict)
                        self.config[key]['value'] = result
                        logger.info(f"Resolved template for {key}: {expression} => {result}")
                    except Exception as e:
                        logger.error(f"Failed to resolve template for {key}: {expression}. Error: {e}")
                        # Keep the original template on error
                else:
                    logger.warning(f"Invalid template format for {key}: {template}")
    
    def _build_hierarchical_config(self):
        """
        Build a hierarchical configuration structure.
        
        This groups child configurations under their parent keys.
        """
        # First pass: create entries for all keys
        for key, config in self.config.items():
            # Skip disabled configs
            if not config['is_enabled']:
                continue
                
            # Add to hierarchical structure
            self.hierarchical_config[key] = {
                'value': config['value'],
                'children': {}
            }
        
        # Second pass: add children to their parents
        for key, config in self.config.items():
            # Skip disabled configs
            if not config['is_enabled']:
                continue
                
            parent_key = config['parent_key']
            if parent_key and parent_key in self.hierarchical_config:
                # Add as child to parent
                self.hierarchical_config[parent_key]['children'][key] = {
                    'value': config['value']
                }
    
    def _cache_config(self):
        """
        Cache config to file for DBT to read.
        Creates both a project-specific and chain-specific cache file.
        """
        # Determine cache directory location (in logs/config_cache)
        fsc_evm_dir = self._find_fsc_evm_dir()
        cache_dir = os.path.join(fsc_evm_dir, 'logs', 'config_cache')
        os.makedirs(cache_dir, exist_ok=True)
        
        # Prepare flat version for DBT (just key:value pairs)
        flat_config = {k: v['value'] for k, v in self.config.items() if v['is_enabled']}
        
        # Cache by project name (flat format)
        project_cache_path = os.path.join(cache_dir, f'config_{self.project_name}.json')
        with open(project_cache_path, 'w') as f:
            json.dump(flat_config, f, indent=2)
        
        # Cache by chain name (flat format)
        chain_cache_path = os.path.join(cache_dir, f'config_chain_{self.chain_name}.json')
        with open(chain_cache_path, 'w') as f:
            json.dump(flat_config, f, indent=2)
        
        # Also cache hierarchical format
        hier_cache_path = os.path.join(cache_dir, f'config_hierarchical_{self.chain_name}.json')
        with open(hier_cache_path, 'w') as f:
            json.dump(self.hierarchical_config, f, indent=2)
            
        logger.info(f"Configuration cached to {project_cache_path}, {chain_cache_path}, and {hier_cache_path}")
    
    def _find_fsc_evm_dir(self):
        """
        Find the FSC-EVM repository directory.
        
        Returns:
            str: Path to the FSC-EVM directory
        """
        # Start with current directory and walk up
        current_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Go up to analysis/config parent (should be FSC-EVM root)
        fsc_evm_dir = os.path.dirname(os.path.dirname(current_dir))
        
        # Verify we found the right directory by checking for dbt_project.yml
        if os.path.exists(os.path.join(fsc_evm_dir, 'dbt_project.yml')):
            return fsc_evm_dir
        
        # If not found, raise error
        raise ValueError("Could not locate FSC-EVM repository root directory")
    
    def get(self, key, default=None):
        """
        Get a configuration value.
        
        Args:
            key (str): The configuration key to retrieve
            default: The default value to return if key not found
            
        Returns:
            The configuration value, or default if not found
        """
        config_entry = self.config.get(key)
        if config_entry and config_entry['is_enabled']:
            return config_entry['value']
        return default
    
    def get_children(self, parent_key):
        """
        Get all children configuration values for a parent key.
        
        Args:
            parent_key (str): The parent configuration key
            
        Returns:
            dict: A dictionary of child key:value pairs
        """
        result = {}
        
        # Look up parent in hierarchical config
        parent = self.hierarchical_config.get(parent_key)
        if parent and 'children' in parent:
            for child_key, child_config in parent['children'].items():
                result[child_key] = child_config['value']
                
        return result
    
    def get_all(self):
        """
        Get all configuration values in a flat dictionary.
        
        Returns:
            dict: A dictionary of all enabled configuration values
        """
        return {k: v['value'] for k, v in self.config.items() if v['is_enabled']}
    
    def get_hierarchical(self):
        """
        Get the full hierarchical configuration.
        
        Returns:
            dict: The hierarchical configuration structure
        """
        return self.hierarchical_config.copy()

# Singleton instance for easy access
_instance = None

def get_instance(project_name=None):
    """
    Get or create the ConfigManager instance.
    
    Args:
        project_name (str, optional): The project name to use
        
    Returns:
        ConfigManager: The configuration manager instance
    """
    global _instance
    if _instance is None:
        _instance = ConfigManager(project_name)
    return _instance

def get_config(key, default=None):
    """
    Utility function to get a configuration value.
    
    Args:
        key (str): The configuration key to retrieve
        default: The default value to return if key not found
        
    Returns:
        The configuration value, or default if not found
    """
    return get_instance().get(key, default)

def get_children(parent_key):
    """
    Utility function to get all children for a parent key.
    
    Args:
        parent_key (str): The parent key
        
    Returns:
        dict: A dictionary of child configurations
    """
    return get_instance().get_children(parent_key)