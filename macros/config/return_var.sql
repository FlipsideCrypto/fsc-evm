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
            'GLOBAL_PROD_DB_NAME': '',
            'CHAINHEAD_SQL_LIMIT': '2 * MAIN_SL_BLOCKS_PER_HOUR'
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
    
    {# Helper function to resolve variable references #}
    {% macro resolve_value(value, chain) %}
        {% if value is string and ' * ' in value %}
            {% set parts = value.split(' * ') %}
            {% set var_name = parts[1].strip() %}
            {% set multiplier = parts[0].strip() | int %}
            
            {% if chain in config and var_name in config[chain] %}
                {% set referenced_value = config[chain][var_name] %}
            {% elif var_name in config['default'] %}
                {% set referenced_value = config['default'][var_name] %}
            {% else %}
                {% set referenced_value = 0 %}
            {% endif %}
            
            {% if referenced_value is number %}
                {{ return(multiplier * referenced_value) }}
            {% else %}
                {{ return(value) }}  {# Return as is if not a number #}
            {% endif %}
        {% else %}
            {{ return(value) }}
        {% endif %}
    {% endmacro %}
    
    {# Check if the key exists in our config #}
    {% if chain_name in config and key in config[chain_name] %}
        {% set value = config[chain_name][key] %}
        {{ return(resolve_value(value, chain_name)) }}
    {% elif key in config['default'] %}
        {# Fall back to global config if the key exists there #}
        {% set value = config['default'][key] %}
        {{ return(resolve_value(value, chain_name)) }}
    {% else %}
        {# Return the default if the key isn't found anywhere #}
        {{ return(default) }}
    {% endif %}
{% endmacro %}