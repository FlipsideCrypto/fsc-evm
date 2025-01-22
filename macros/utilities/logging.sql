{% macro log_model_details(vars=false, params=false) %}

{%- if flags.WHICH == 'compile' and execute -%}
/* 
DBT Model Config:
{{ model.config | tojson(indent=2) }}
*/
    
{% if vars is not false %}
/*
Variables:
{{ vars | tojson(indent=2) }}
*/
{% endif %}

{% if params is not false %}
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