{% macro set_streamline_parameters_decoder(model_name, model_type) %}

{%- set params = {
    "external_table": var((model_name ~ '_external_table').upper(), model_name.lower()),
    "sql_limit": var((model_name ~ '_sql_limit').upper(), 0),
    "producer_batch_size": var((model_name ~ '_producer_batch_size').upper(), 0),
    "worker_batch_size": var((model_name ~ '_worker_batch_size').upper(), 0),
    "sql_source": (model_name ~ '_' ~ model_type).lower()
} -%}

{{ return(params) }}

{% endmacro %}