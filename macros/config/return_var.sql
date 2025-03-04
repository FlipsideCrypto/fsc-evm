{% macro return_var(key, default='') %}
    {# 
    Enhanced version that:
    1. Uses the project name from dbt_project.yml
    2. Supports nested configs under chain-specific names
    3. Falls back to global defaults when a specific chain config isn't available
    4. Supports expressions in the default parameter
    #}
    
    {# Extract chain name from the project name #}
    {% set project_name = project_name %}
    {% set chain_name = project_name.split('_')[0] if '_' in project_name else project_name %}
    
    {# Define our configuration structure #}
    {% set config = {
        'default': {
            'GLOBAL_CHAIN_NETWORK': 'unknown',
            'MAIN_SL_BLOCKS_PER_HOUR': 0,
            'GLOBAL_PROD_DB_NAME': ''
        },
        'mantle': {
            'GLOBAL_CHAIN_NETWORK': 'mantle',
            'MAIN_SL_BLOCKS_PER_HOUR': 1800,
            'GLOBAL_PROD_DB_NAME': 'mantle'
        },
        'swell': {
            'GLOBAL_CHAIN_NETWORK': 'swell',
            'MAIN_SL_BLOCKS_PER_HOUR': 1500,
            'GLOBAL_PROD_DB_NAME': 'swell'
        },
        'ethereum': {
            'GLOBAL_CHAIN_NETWORK': 'ethereum',
            'MAIN_SL_BLOCKS_PER_HOUR': 300,
            'GLOBAL_PROD_DB_NAME': 'ethereum'
        }
    } %}
    
    {# Check if the key exists in our config #}
    {% if chain_name in config and key in config[chain_name] %}
        {{ return(config[chain_name][key]) }}
    {% elif key in config['default'] %}
        {# Fall back to default config if the key exists there #}
        {{ return(config['default'][key]) }}
    {% else %}
        {# Return the default if the key isn't found anywhere #}
        {{ return(default) }}
    {% endif %}
{% endmacro %}