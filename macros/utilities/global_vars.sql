{% macro set_global_vars() %}
    {% set vars = return_vars() %}
    {% do context.update({"vars": vars}) %}
{% endmacro %}