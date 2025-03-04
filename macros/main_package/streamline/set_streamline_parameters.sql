{% macro set_streamline_parameters(package_name, model_name, model_type, multiplier=1) %}

{%- set vars = return_vars() -%}

{%- set rpc_config_details = {
    "blocks_transactions": {
        "method": 'eth_getBlockByNumber',
        "method_params": 'ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), TRUE)',
        "exploded_key": ['result', 'result.transactions']
    },
    "receipts_by_hash": {
        "method": 'eth_getTransactionReceipt',
        "method_params": 'ARRAY_CONSTRUCT(tx_hash)'
    },
    "receipts": {
        "method": 'eth_getBlockReceipts',
        "method_params": 'ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number))',
        "exploded_key": ['result'],
        "lambdas": 2

    },
    "traces": {
        "method": 'debug_traceBlockByNumber',
        "method_params": "ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), OBJECT_CONSTRUCT('tracer', 'callTracer', 'timeout', '120s'))",
        "exploded_key": ['result'],
        "lambdas": 2
    },
    "confirm_blocks": {
        "method": 'eth_getBlockByNumber',
        "method_params": 'ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), FALSE)'
    }
} -%}

{%- set rpc_config = rpc_config_details[model_name.lower()] -%}

{%- set model_var_prefix = (package_name ~ '_SL_' ~ model_name ~ '_' ~ model_type).upper() -%}

{%- set external_table_var = model_var_prefix ~ '_external_table' -%}
{%- set sql_limit_var = model_var_prefix ~ '_sql_limit' -%}
{%- set producer_batch_size_var = model_var_prefix ~ '_producer_batch_size' -%}
{%- set worker_batch_size_var = model_var_prefix ~ '_worker_batch_size' -%}
{%- set async_concurrent_requests_var = model_var_prefix ~ '_async_concurrent_requests' -%}

{%- set params = {
    "external_table": vars[external_table_var] if external_table_var in vars else model_name.lower(),
    "sql_limit": vars[sql_limit_var],
    "producer_batch_size": vars[producer_batch_size_var],
    "worker_batch_size": vars[worker_batch_size_var],
    "sql_source": (model_name ~ '_' ~ model_type).lower(),
    "method": rpc_config['method'],
    "method_params": rpc_config['method_params']
} -%}

{%- if async_concurrent_requests_var in vars -%}
    {%- do params.update({"async_concurrent_requests": vars[async_concurrent_requests_var]}) -%}
{%- endif -%}

{%- if rpc_config.get('exploded_key') is not none -%}
    {%- do params.update({"exploded_key": tojson(rpc_config['exploded_key'])}) -%}
{%- endif -%}

{%- if rpc_config.get('lambdas') is not none -%}
    {%- do params.update({"lambdas": rpc_config['lambdas']}) -%}
{%- endif -%}

{{ return(params) }}

{% endmacro %}