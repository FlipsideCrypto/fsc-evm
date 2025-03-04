{% macro get_config_v1(key, default='') %}
    {# 
    Enhanced version that:
    1. Auto-detects the current project name from the file path
    2. Supports nested configs under chain-specific models
    3. Falls back to global defaults when a specific chain config isn't available
    #}
    
    {# Extract project name from path #}
    {% set project_path = this.path %}
    {% set project_name = '' %}
    
    {% if project_path %}
        {# Extract the project name from the path - typically in format "path/to/[project-name]-models/..." #}
        {% set path_parts = project_path.split('/') %}
        {% for part in path_parts %}
            {% if part.endswith('-models') %}
                {% set project_name = part.split('-models')[0] %}
                {% break %}
            {% endif %}
        {% endfor %}
    {% endif %}
    
    {# If we couldn't detect the project name, default to 'mantle' #}
    {% if project_name == '' %}
        {% set project_name = 'mantle' %}
    {% endif %}
    
    {# Define our nested configuration structure #}
    {% set config = {
        'mantle-models': {
            'GLOBAL_CHAIN_NETWORK': 'mantle',
            'MAIN_SL_BLOCKS_PER_HOUR': 1800,
            'GLOBAL_PROD_DB_NAME': 'mantle'
        },
        'swell-models': {
            'GLOBAL_CHAIN_NETWORK': 'swell',
            'MAIN_SL_BLOCKS_PER_HOUR': 1500,
            'GLOBAL_PROD_DB_NAME': 'swell'
        },
        'ethereum-models': {
            'GLOBAL_CHAIN_NETWORK': 'ethereum',
            'MAIN_SL_BLOCKS_PER_HOUR': 300,
            'GLOBAL_PROD_DB_NAME': 'ethereum'
        },
        'global': {
            'GLOBAL_CHAIN_NETWORK': 'unknown',
            'MAIN_SL_BLOCKS_PER_HOUR': 0,
            'GLOBAL_PROD_DB_NAME': ''
        }
    } %}
    
    {# Try to get the value from project-specific config #}
    {% set project_key = project_name + '-models' %}
    
    {% if project_key in config and key in config[project_key] %}
        {{ return(config[project_key][key]) }}
    {% elif key in config['global'] %}
        {# Fall back to global config if the key exists there #}
        {{ return(config['global'][key]) }}
    {% else %}
        {# Return the default if the key isn't found anywhere #}
        {{ return(default) }}
    {% endif %}
{% endmacro %}