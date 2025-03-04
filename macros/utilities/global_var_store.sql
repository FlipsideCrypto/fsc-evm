{% macro _get_global_vars() %}
  {% do return(return_vars()) %}
{% endmacro %}

{% macro get_global_vars() %}
  {# Use the global dictionary to cache variable results #}
  {% if not context.get('_cached_global_vars') %}
    {% do context.update({'_cached_global_vars': _get_global_vars()}) %}
    {% do log("Global variables initialized for the first time", info=true) %}
  {% else %}
    {% do log("Using cached global variables", info=true) %}
  {% endif %}
  
  {% do return(context._cached_global_vars) %}
{% endmacro %}