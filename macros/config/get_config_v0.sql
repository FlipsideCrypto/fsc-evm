{% macro get_config_v0(key, default='') %}
    {# 
    Since we need a direct file access method without modules.os, 
    we'll use a hardcoded approach for the first version.
    Configuration is now organized by project/blockchain.
    #}
    
    {% set config = {
        'mantle-models': {
            'CHAIN_NETWORK': "'mantle'",
            'BLOCKS_PER_HOUR': '1800',
            'PROD_DB_NAME': "'mantle'"
        },
        'ethereum-models': {
            'CHAIN_NETWORK': "'ethereum'",
            'BLOCKS_PER_HOUR': '225',
            'PROD_DB_NAME': "'ethereum'"
        },
        'base-models': {
            'CHAIN_NETWORK': "'base'",
            'BLOCKS_PER_HOUR': '1800',
            'PROD_DB_NAME': "'base'"
        }
    } %}

    {# Get the project name from the target database #}
    {% set project = target.database.split('.')[0] ~ '-models' %}
    
    {# Split the key to check if it's a global parameter or project-specific #}
    {% set key_parts = key.split('.') %}
    
    {% if key_parts | length == 1 %}
        {# If no project specified in the key, use the current project #}
        {% if project in config and key in config[project] %}
            {{ return(config[project][key]) }}
        {% else %}
            {{ return(default) }}
        {% endif %}
    {% elif key_parts | length == 2 %}
        {# If project is specified in the key (e.g., 'ethereum-models.BLOCKS_PER_HOUR') #}
        {% set project_name = key_parts[0] %}
        {% set param_key = key_parts[1] %}
        {% if project_name in config and param_key in config[project_name] %}
            {{ return(config[project_name][param_key]) }}
        {% else %}
            {{ return(default) }}
        {% endif %}
    {% else %}
        {{ return(default) }}
    {% endif %}
{% endmacro %}