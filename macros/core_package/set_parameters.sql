{% macro set_streamline_parameters(model_name, model_type, exploded_key=none, lambdas=1, multiplier=1) %}

{%- set params = {
    "external_table": var((model_name ~ '_' ~ model_type ~ '_external_table').upper(), model_name.lower()),
    "sql_limit": var((model_name ~ '_' ~ model_type ~ '_sql_limit').upper(), 2 * var('GLOBAL_BLOCKS_PER_HOUR') * multiplier),
    "producer_batch_size": var((model_name ~ '_' ~ model_type ~ '_producer_batch_size').upper(), 2 * var('GLOBAL_BLOCKS_PER_HOUR') * multiplier),
    "worker_batch_size": var(
        (model_name ~ '_' ~ model_type ~ '_worker_batch_size').upper(), 
        (2 * var('GLOBAL_BLOCKS_PER_HOUR') * multiplier) // lambdas
    ),
    "sql_source": (model_name ~ '_' ~ model_type).lower()
} -%}

{%- if exploded_key is not none -%}
    {%- do params.update({"exploded_key": tojson(exploded_key)}) -%}
{%- endif -%}

{{ return(params) }}

{% endmacro %}