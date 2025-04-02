{% macro log_model_details() %}

{%- if execute -%}
/* 
DBT Model Config:
{{ model.config | tojson(indent=2) }}
*/
    
/*
Raw Code:
{{ model.raw_code }}
*/
{%- endif -%}
{% endmacro %}