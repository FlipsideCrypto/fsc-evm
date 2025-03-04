{% macro init_global_vars() %}
  {% if not adapter.config.has_node('global_vars') %}
    {% do adapter.config.set_node('global_vars', return_vars()) %}
    {% do log("Global variables initialized", info=true) %}
  {% endif %}
{% endmacro %}

{% macro get_global_vars() %}
  {{ init_global_vars() }}
  {{ return(adapter.config.get_node('global_vars')) }}
{% endmacro %}