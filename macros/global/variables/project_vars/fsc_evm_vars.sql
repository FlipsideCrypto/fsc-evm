{% macro fsc_evm_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'fsc_evm'
    } %}
    
    {{ return(vars) }}
{% endmacro %} 