{% macro log_model_details(vars=false, params=false) %}

{%- if execute -%}
/* 
DBT Model Config:
{{ model.config | tojson(indent=2) }}
*/
    
{% if vars is not false %}

{% if var('GLOBAL_LOGGING_MODEL_LOGS_ENABLED', false) %}
{{ log( vars | tojson(indent=2), info=True) }}
{% endif %}
/*
Variables:
{{ vars | tojson(indent=2) }}
*/
{% endif %}

{% if params is not false %}

{% if var('GLOBAL_LOGGING_MODEL_LOGS_ENABLED', false) %}
{{ log( params | tojson(indent=2), info=True) }}
{% endif %}
/*
Parameters: 
{{ params | tojson(indent=2) }}
*/
{% endif %}

/*
Raw Code:
{{ model.raw_code }}
*/
{%- endif -%}
{% endmacro %}