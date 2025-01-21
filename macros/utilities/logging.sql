{% macro log_model_config(streamline_params=false, default_vars=false) %}

{%- if flags.WHICH == 'compile' and execute -%}
/* 
DBT Model Config:
{{ model.config | tojson(indent=2) }}
*/
    
{% if streamline_params %}
/*
Streamline Parameters: 
{{ streamline_params | tojson(indent=2) }}
*/
{% endif %}

{% if default_vars %}
/*
Default Variables:
{{ default_vars | tojson(indent=2) }}
*/

{% endif %}

/*
Raw Code:
{{ model.raw_code }}
*/
{%- endif -%}
{% endmacro %}
