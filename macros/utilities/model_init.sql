{% macro model_init(vars=false, params=false) %}
    {# Initialize variables if needed #}
    {% if var('GLOBAL_INIT_REQUIRED', true) %}
        {{ return_vars() }}
    {% endif %}
    
    {# Log model details #}
    {{ log_model_details(vars, params) }}
{% endmacro %} 