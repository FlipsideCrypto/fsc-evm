{% macro cached_vars() %}
  {# This creates a global namespace that persists across macro calls #}
  {% if not varargs %}
    {% set varargs = namespace() %}
  {% endif %}

  {# Only calculate vars once and store in the "cache" #}
  {% if not varargs.cached_variables_set %}
    {% set varargs.vars = return_vars() %}
    {% set varargs.cached_variables_set = true %}
    {% do log("Global variables calculated and cached", info=true) %}
  {% else %}
    {% do log("Using cached variables", info=true) %}
  {% endif %}

  {{ return(varargs.vars) }}
{% endmacro %}