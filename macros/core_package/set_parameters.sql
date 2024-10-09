{% macro set_streamline_parameters(model_name, model_type, multiplier=1) %}

{%- set rpc_config_details = {
    "block_transactions": {
        "method": 'eth_getBlockByNumber',
        "params": 'ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), TRUE)',
        "exploded_key": ['data', 'result.transactions']
    },
    "receipts_by_hash": {
        "method": 'eth_getTransactionReceipt',
        "params": 'ARRAY_CONSTRUCT(tx_hash)'
    },
    "receipts": {
        "method": 'eth_getBlockReceipts',
        "params": 'ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number))',
        "exploded_key": ['result'],
        "lambdas": 2

    },
    "traces": {
        "method": 'debug_traceBlockByNumber',
        "params": "ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), OBJECT_CONSTRUCT('tracer', 'callTracer', 'timeout', '120s'))",
        "exploded_key": ['result'],
        "lambdas": 2
    },
    "confirm_blocks": {
        "method": 'eth_getBlockByNumber',
        "params": 'ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), FALSE)'
    }
} -%}

{%- set rpc_config = rpc_config_details[model_name.lower()] -%}

{%- set params = {
    "external_table": var((model_name ~ '_' ~ model_type ~ '_external_table').upper(), model_name.lower()),
    "sql_limit": var((model_name ~ '_' ~ model_type ~ '_sql_limit').upper(), 2 * var('GLOBAL_BLOCKS_PER_HOUR') * multiplier),
    "producer_batch_size": var((model_name ~ '_' ~ model_type ~ '_producer_batch_size').upper(), 2 * var('GLOBAL_BLOCKS_PER_HOUR') * multiplier),
    "worker_batch_size": var(
        (model_name ~ '_' ~ model_type ~ '_worker_batch_size').upper(), 
        (2 * var('GLOBAL_BLOCKS_PER_HOUR') * multiplier) // (rpc_config['lambdas'] | default(1))
    ),
    "sql_source": (model_name ~ '_' ~ model_type).lower(),
    "method": rpc_config['method'],
    "params": rpc_config['params']
} -%}

{%- if rpc_config['exploded_key'] is not none -%}
    {%- do params.update({"exploded_key": tojson(rpc_config['exploded_key'])}) -%}
{%- endif -%}

{%- if rpc_config['lambdas'] is not none -%}
    {%- do params.update({"lambdas": rpc_config['lambdas']}) -%}
{%- endif -%}

{{ return(params) }}

{% endmacro %}