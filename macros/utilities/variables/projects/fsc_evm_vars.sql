{% macro fsc_evm_vars() %}
    {% set vars = {
        'GLOBAL_PROD_DB_NAME': 'fsc_evm'
    } %}
    
    {{ return(vars) }}
{% endmacro %} 