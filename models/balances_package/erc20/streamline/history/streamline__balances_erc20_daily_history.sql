{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = "view",
    tags = ['streamline','balances','history','erc20','phase_4']
) }}

SELECT
    block_number,
    block_date_unix,
    address,
    contract_address,
    partition_key,
    live.udf_api(
        'POST',
        '{{ vars.GLOBAL_NODE_URL }}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json',
            'fsc-quantum-state',
            'streamline'
        ),
        api_request,
        '{{ vars.GLOBAL_NODE_VAULT_PATH }}'
    ) AS request
FROM
    {{ ref('streamline__balances_erc20_daily_history_to_do') }}
ORDER BY partition_key DESC, block_number DESC

LIMIT {{ vars.BALANCES_SL_ERC20_DAILY_HISTORY_SQL_LIMIT }}

{# Streamline Function Call #}
{% if execute %}
    {% set params = {
        "external_table": 'balances_erc20',
        "sql_limit": vars.BALANCES_SL_ERC20_DAILY_HISTORY_SQL_LIMIT,
        "producer_batch_size": vars.BALANCES_SL_ERC20_DAILY_HISTORY_PRODUCER_BATCH_SIZE,
        "worker_batch_size": vars.BALANCES_SL_ERC20_DAILY_HISTORY_WORKER_BATCH_SIZE,
        "async_concurrent_requests": vars.BALANCES_SL_ERC20_DAILY_HISTORY_ASYNC_CONCURRENT_REQUESTS,
        "sql_source": 'balances_erc20_daily_history'
    } %}

    {% set function_call_sql %}
    {{ fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = this.schema ~ "." ~ this.identifier,
        params = params
    ) }}
    {% endset %}

    {% do run_query(function_call_sql) %}
    {{ log("Streamline function call: " ~ function_call_sql, info=true) }}
{% endif %}