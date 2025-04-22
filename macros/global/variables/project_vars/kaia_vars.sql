{% macro kaia_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'kaia'
    } %}
    
    {{ return(vars) }}
{% endmacro %} 