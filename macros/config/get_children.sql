{% macro get_children(parent_key) %}
    {#
        Retrieve all child configuration values for a parent key.
        This is useful for hierarchical configurations like token mappings.
        
        Args:
            parent_key (str): The parent configuration key
            
        Returns:
            dict: A dictionary of child key:value pairs
    #}
    
    {% if execute %}
        -- Try to get children from the config module
        {% set config_module = modules.importlib.import_module('analysis.config') %}
        {% set children = config_module.get_children(parent_key) %}
        
        {% if children %}
            {{ return(children) }}
        {% endif %}
        
        -- If not found, try to look it up in the cached hierarchical config file
        {% set project_name = project_name %}
        {% set fsc_evm_dir = modules.os.path.dirname(project_path) %}
        
        -- Get the chain name from config
        {% set chain_config_path = modules.os.path.join(fsc_evm_dir, 'logs', 'config_cache', 'config_' ~ project_name ~ '.json') %}
        {% set chain_name = '' %}
        
        {% if modules.os.path.exists(chain_config_path) %}
            {% set config_file = open(chain_config_path, 'r') %}
            {% set config = modules.json.loads(config_file.read()) %}
            {% do config_file.close() %}
            
            {% if 'GLOBAL_CHAIN_NETWORK' in config %}
                {% set chain_name = config['GLOBAL_CHAIN_NETWORK'] %}
            {% endif %}
        {% endif %}
        
        -- If we have a chain name, look up the hierarchical config
        {% if chain_name %}
            {% set hier_config_path = modules.os.path.join(fsc_evm_dir, 'logs', 'config_cache', 'config_hierarchical_' ~ chain_name ~ '.json') %}
            
            {% if modules.os.path.exists(hier_config_path) %}
                {% set config_file = open(hier_config_path, 'r') %}
                {% set hier_config = modules.json.loads(config_file.read()) %}
                {% do config_file.close() %}
                
                {% if parent_key in hier_config and 'children' in hier_config[parent_key] %}
                    {% set result = {} %}
                    {% for child_key, child_data in hier_config[parent_key]['children'].items() %}
                        {% do result.update({child_key: child_data['value']}) %}
                    {% endfor %}
                    {{ return(result) }}
                {% endif %}
            {% endif %}
        {% endif %}
        
        -- If still not found, return empty dict
        {{ return({}) }}
    {% else %}
        {{ return({}) }}
    {% endif %}
{% endmacro %}