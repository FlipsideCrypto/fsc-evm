{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = "view",
    tags = ['streamline','balances','history','erc20','phase_4']
) }}

WITH last_x_days AS (

    SELECT
        block_number,
        block_date
    FROM
        {{ ref("_max_block_by_date") }}
    WHERE block_date > {{ vars.BALANCES_SL_START_DATE }}
),
to_do AS (
    SELECT
        DISTINCT
        d.block_number,
        d.block_date,
        t.address,
        t.contract_address
    FROM
        {{ ref("streamline__balances_erc20_daily_records") }} t
        INNER JOIN last_x_days d 
            ON t.block_date = d.block_date
    --max daily block_number during the selected period, for each contract_address/address pair
    WHERE
        t.block_date IS NOT NULL
        And d.block_date < (
            SELECT MAX(block_date)
            FROM last_x_days
        ) 
    EXCEPT
    SELECT
        block_number,
        block_date,
        address,
        contract_address
    FROM
        {{ ref("streamline__balances_erc20_daily_complete") }}
    WHERE block_date IS NOT NULL
    AND block_date < (
        SELECT MAX(block_date)
        FROM last_x_days
    )
)
SELECT
    block_number,
    DATE_PART('EPOCH_SECONDS', block_date) :: INT AS block_date_unix,
    address,
    contract_address,
    ROUND(
        block_number,
        -3
    ) AS partition_key,
    live.udf_api(
        'POST',
        '{{ vars.GLOBAL_NODE_URL }}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json',
            'fsc-quantum-state',
            'streamline'
        ),
        OBJECT_CONSTRUCT(
            'id',
            CONCAT(
                contract_address,
                '-',
                address,
                '-',
                block_number
            ),
            'jsonrpc',
            '2.0',
            'method',
            'eth_call',
            'params',
            ARRAY_CONSTRUCT(
                OBJECT_CONSTRUCT(
                    'to',
                    contract_address,
                    'data',
                    CONCAT(
                        '0x70a08231000000000000000000000000',
                        SUBSTR(
                            address,
                            3
                        )
                    )
                ),
                utils.udf_int_to_hex(block_number)
            )
        ),
        '{{ vars.GLOBAL_NODE_VAULT_PATH }}'
    ) AS request
FROM
    to_do
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