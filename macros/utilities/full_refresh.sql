{% macro full_refresh(enabled=false) %}
    {{ return(none if enabled else false) }}
{% endmacro %}