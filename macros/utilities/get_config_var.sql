{% macro get_config_var(var_name, default=none) %}
  {% set vars_dict = {} %}
  {{ return(vars_dict.get(var_name, default)) }}
{% endmacro %}