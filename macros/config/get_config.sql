{% macro get_config(key, default='') %}
    {# 
    Since we need a direct file access method without modules.os, 
    we'll use a hardcoded approach for the first version.
    #}
    
    {% set config = {
        'GLOBAL_CHAIN_NETWORK': 'mantle',
        'MAIN_SL_BLOCKS_PER_HOUR': 1800,
        'GLOBAL_PROD_DB_NAME': 'mantle'
    } %}
    
    {% if key in config %}
        {{ return(config[key]) }}
    {% else %}
        {{ return(default) }}
    {% endif %}
{% endmacro %}