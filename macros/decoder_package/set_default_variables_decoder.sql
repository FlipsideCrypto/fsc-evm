{% macro set_default_variables_streamline_decoder(model_name, model_type) %}

{%- set testing_limit = var((model_name ~ '_' ~ model_type ~ '_testing_limit').upper(), none) -%}

{%- set variables = {
    'testing_limit': testing_limit
} -%}

{{ return(variables) }}

{% endmacro %}