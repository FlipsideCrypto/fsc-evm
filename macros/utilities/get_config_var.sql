{% macro get_config_var(var_name, default=none) %}
  {# Cache the variables dictionary to avoid repeated calls to return_vars() #}
  {% if not execute %}
    {{ return(default) }}
  {% endif %}
  
  {% if not adapter.dispatch('get_config_var_dict')() %}
    {% do adapter.dispatch('set_config_var_dict')(return_vars()) %}
  {% endif %}
  
  {% set vars_dict = adapter.dispatch('get_config_var_dict')() %}
  {{ return(vars_dict.get(var_name, default)) }}
{% endmacro %}

{% macro default__get_config_var_dict() %}
  {% do return(var('_config_vars_dict', {})) %}
{% endmacro %}

{% macro default__set_config_var_dict(dict_value) %}
  {% do var('_config_vars_dict', dict_value) %}
{% endmacro %}