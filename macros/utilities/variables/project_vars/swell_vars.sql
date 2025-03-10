{% macro swell_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'swell',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/swell/ankr/mainnet'
    } %}
    
    {{ return(vars) }}
{% endmacro %} 