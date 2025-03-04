{% macro return_var(key, default='') %}
    {# 
    Enhanced version that:
    1. Uses the project name from dbt_project.yml
    2. Supports nested configs under chain-specific names
    3. Falls back to global defaults when a specific chain config isn't available
    4. Supports expressions in the default parameter
    5. Supports referencing other variables with special syntax
    #}
    
    {# Extract chain name from the project name #}
    {% set project_name = project_name %}
    {% set chain_name = project_name.split('_')[0] if '_' in project_name else project_name %}
    
    {# Define our configuration structure #}
    {% set config = {
        'global': {
            'GLOBAL_CHAIN_NETWORK': 'unknown',
            'MAIN_SL_BLOCKS_PER_HOUR': 0,
            'GLOBAL_PROD_DB_NAME': '',
            'CHAINHEAD_SQL_LIMIT': '2 * MAIN_SL_BLOCKS_PER_HOUR',  # Reference to another variable
            'VERTEX_CONTRACTS': {
                'ABI': '0x0000000000000000000000000000000000000000',
                'ADDRESS': '0x0000000000000000000000000000000000000000'
            }
        },
        'mantle': {
            'GLOBAL_CHAIN_NETWORK': 'mantle',
            'MAIN_SL_BLOCKS_PER_HOUR': 1800,
            'GLOBAL_PROD_DB_NAME': 'mantle'
        },
        'swell': {
            'GLOBAL_CHAIN_NETWORK': 'swell',
            'MAIN_SL_BLOCKS_PER_HOUR': 1500,
            'GLOBAL_PROD_DB_NAME': 'swell',
            'VERTEX_CONTRACTS': {
                'ABI': '0x0000000000000000000000000000000000000000',
                'ADDRESS': '0x0000000000000000000000000000000000000000'
            }
        },
        'ethereum': {
            'GLOBAL_CHAIN_NETWORK': 'ethereum',
            'MAIN_SL_BLOCKS_PER_HOUR': 300,
            'GLOBAL_PROD_DB_NAME': 'ethereum',
            'VERTEX_CONTRACTS': {
                'ABI': '0x0000000000000000000000000000000000000000',
                'ADDRESS': '0x0000000000000000000000000000000000000000'
            }
        }
    } %}
    
    {# Get the value for the key from the appropriate config #}
    {% if chain_name in config and key in config[chain_name] %}
        {% set value = config[chain_name][key] %}
    {% elif key in config['global'] %}
        {% set value = config['global'][key] %}
    {% else %}
        {{ return(default) }}
    {% endif %}
    
    {# Resolve variable references if needed #}
    {% if value is string and ' * ' in value %}
        {% set parts = value.split(' * ') %}
        {% set multiplier = parts[0].strip() | int %}
        {% set var_name = parts[1].strip() %}
        
        {# Get the referenced variable value #}
        {% if chain_name in config and var_name in config[chain_name] %}
            {% set referenced_value = config[chain_name][var_name] %}
        {% elif var_name in config['global'] %}
            {% set referenced_value = config['global'][var_name] %}
        {% else %}
            {% set referenced_value = 0 %}
        {% endif %}
        
        {# Calculate the final value #}
        {% if referenced_value is number %}
            {{ return(multiplier * referenced_value) }}
        {% else %}
            {{ return(value) }}
        {% endif %}
    {% else %}
        {{ return(value) }}
    {% endif %}
{% endmacro %}