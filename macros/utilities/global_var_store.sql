{% macro _get_global_vars() %}
  {% do return(return_vars()) %}
{% endmacro %}

{% macro get_global_vars() %}
  {% set cache_file = '/tmp/dbt_vars_cache_' ~ project_name ~ '.json' %}
  
  {% if execute %}
    {% set file_exists = modules.os.path.exists(cache_file) %}
    
    {% if file_exists %}
      {% do log("Reading vars from cache file", info=true) %}
      {% set f = modules.builtins.open(cache_file, 'r') %}
      {% set cached_vars = fromjson(f.read()) %}
      {% do f.close() %}
      {{ return(cached_vars) }}
    {% else %}
      {% do log("Cache file not found, initializing global vars", info=true) %}
      {% set global_vars = _get_global_vars() %}
      
      {% set f = modules.builtins.open(cache_file, 'w') %}
      {% do f.write(tojson(global_vars)) %}
      {% do f.close() %}
      
      {{ return(global_vars) }}
    {% endif %}
  {% else %}
    {{ return({}) }}
  {% endif %}
{% endmacro %}