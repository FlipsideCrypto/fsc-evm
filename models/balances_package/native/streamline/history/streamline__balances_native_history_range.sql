{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = "view",
    tags = ['streamline','balances','history_range','native','phase_4']
) }}

WITH last_x_days AS (

    SELECT
        block_number,
        block_timestamp
    FROM
        {{ ref("_max_block_by_date") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_number DESC
        ) BETWEEN 2 AND 91 --from 90 days ago to 2 days ago
),
traces AS (
    SELECT
        block_number,
        from_address AS address1,
        to_address AS address2
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        value > 0
        AND type NOT IN (
            'DELEGATECALL',
            'STATICCALL'
        )
        AND block_number >= (
            SELECT MIN(block_number)
            FROM last_x_days
        )
        AND block_number <= (
            SELECT MAX(block_number)
            FROM last_x_days
        )
        --only include traces between selected period
),
tx_fees AS (
    SELECT
        block_number,
        from_address AS address
    FROM
        {{ ref('core__fact_transactions') }}
    WHERE
        tx_fee > 0
        AND from_address <> '0x0000000000000000000000000000000000000000'
        AND block_number >= (
            SELECT MIN(block_number)
            FROM last_x_days
        )
        AND block_number <= (
            SELECT MAX(block_number)
            FROM last_x_days
        )
        --only include txns between selected period
),
native_transfers AS (
    SELECT
        DISTINCT address1 AS address
    FROM
        traces
    WHERE
        address1 <> '0x0000000000000000000000000000000000000000'
    UNION
    SELECT
        DISTINCT address2 AS address
    FROM
        traces
    WHERE
        address2 <> '0x0000000000000000000000000000000000000000'
    UNION ALL
    SELECT
        DISTINCT address
    FROM
        tx_fees
),
to_do AS (
    SELECT
        block_number,
        block_timestamp,
        address
    FROM
        native_transfers t
    CROSS JOIN last_x_days d 
        --max daily block_number during the selected period, for each address
    WHERE
        block_number IS NOT NULL
    EXCEPT
    SELECT
        block_number,
        block_timestamp,
        address
    FROM
        {{ ref("streamline__balances_native_complete") }}
    WHERE
        block_number <= (
            SELECT
                MAX(block_number)
            FROM
                last_x_days
        )
        AND block_number IS NOT NULL
)
SELECT
    block_number,
    block_timestamp,
    address,
    ROUND(
        block_number,
        -3
    ) AS partition_key,
    {{ target.database }}.live.udf_api(
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
                address,
                '-',
                block_number
            ),
            'jsonrpc',
            '2.0',
            'method',
            'eth_getBalance',
            'params',
            ARRAY_CONSTRUCT(address, utils.udf_int_to_hex(block_number))),
        '{{ vars.GLOBAL_NODE_VAULT_PATH }}'
    ) AS request
FROM
    to_do
ORDER BY partition_key DESC, block_number DESC

LIMIT {{ vars.BALANCES_SL_NATIVE_HISTORY_RANGE_SQL_LIMIT }}

{# Streamline Function Call #}
{% if execute %}
    {% set params = {
        "external_table": 'balances_native',
        "sql_limit": vars.BALANCES_SL_NATIVE_HISTORY_RANGE_SQL_LIMIT,
        "producer_batch_size": vars.BALANCES_SL_NATIVE_HISTORY_RANGE_PRODUCER_BATCH_SIZE,
        "worker_batch_size": vars.BALANCES_SL_NATIVE_HISTORY_RANGE_WORKER_BATCH_SIZE,
        "async_concurrent_requests": vars.BALANCES_SL_NATIVE_HISTORY_RANGE_ASYNC_CONCURRENT_REQUESTS,
        "sql_source": 'balances_native_history_range'
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