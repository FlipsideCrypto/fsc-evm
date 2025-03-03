{% macro get_config_var(var_name) %}
  {% set vars_dict = return_vars() %}
  {{ return(vars_dict.get(var_name, none)) }}
{% endmacro %}