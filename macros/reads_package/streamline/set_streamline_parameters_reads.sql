{% macro set_streamline_parameters_reads(model_name, model_type) %}

{%- set rpc_config_details = {
    "reads": {
        "method": 'eth_call',
        "method_params": "ARRAY_CONSTRUCT(OBJECT_CONSTRUCT('to', contract_address, 'data', DATA), utils.udf_int_to_hex(block_number))"
    }
} -%}

{%- set rpc_config = rpc_config_details[model_name.lower()] -%}

{%- set params = {
    "external_table": var((model_name ~ '_' ~ model_type ~ '_external_table').upper(), model_name.lower()),
    "sql_limit": var((model_name ~ '_' ~ model_type ~ '_sql_limit').upper(), 0),
    "producer_batch_size": var((model_name ~ '_' ~ model_type ~ '_producer_batch_size').upper(), 0),
    "worker_batch_size": var((model_name ~ '_' ~ model_type ~ '_worker_batch_size').upper(), 0),
    "sql_source": (model_name ~ '_' ~ model_type).lower(),
    "method": rpc_config['method'],
    "method_params": rpc_config['method_params']
} -%}

{{ return(params) }}

{% endmacro %}