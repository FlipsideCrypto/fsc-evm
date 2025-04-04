{% macro fsc_evm_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'fsc_evm',
        'MAIN_GHA_CHAINHEAD_SCHEDULE': '1,31 * * * *'
    } %}
    
    {{ return(vars) }}
{% endmacro %} 