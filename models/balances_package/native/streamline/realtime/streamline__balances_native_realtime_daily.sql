{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = "view",
    tags = ['streamline','balances','realtime','native','phase_4']
) }}

WITH last_x_days AS (

    SELECT
        block_number,
        block_date
    FROM
        {{ ref("_max_block_by_date") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_number DESC
        ) BETWEEN 1 AND 2 --from 2 days ago and 1 day ago
),
traces AS (
    SELECT
        block_number,
        block_timestamp,
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
        AND block_number > (
            SELECT MIN(block_number)
            FROM last_x_days
        )
        AND block_number <= (
            SELECT MAX(block_number)
            FROM last_x_days
        ) 
        --only include traces from 1 day ago
        AND block_timestamp :: DATE >= DATEADD(
            'day',
            -3,
            SYSDATE()
        )
),
tx_fees AS (
    SELECT
        block_number,
        block_timestamp,
        from_address AS address
    FROM
        {{ ref('core__fact_transactions') }}
    WHERE
        tx_fee > 0
        AND from_address <> '0x0000000000000000000000000000000000000000'
        AND block_number > (
            SELECT MIN(block_number)
            FROM last_x_days
        )
        AND block_number <= (
            SELECT MAX(block_number)
            FROM last_x_days
        ) 
        --only include txns from 1 day ago
        AND block_timestamp :: DATE >= DATEADD(
            'day',
            -3,
            SYSDATE()
        )
),
native_transfers AS (
    SELECT
        DISTINCT 
        block_timestamp :: DATE AS block_date,
        address1 AS address
    FROM
        traces
    WHERE
        address1 <> '0x0000000000000000000000000000000000000000'
    UNION
    SELECT
        DISTINCT 
        block_timestamp :: DATE AS block_date,
        address2 AS address
    FROM
        traces
    WHERE
        address2 <> '0x0000000000000000000000000000000000000000'
    UNION ALL
    SELECT
        DISTINCT 
        block_timestamp :: DATE AS block_date,
        address
    FROM
        tx_fees
),
to_do AS (
    SELECT
        DISTINCT
        d.block_number,
        d.block_date,
        t.address
    FROM
        native_transfers t
        INNER JOIN last_x_days d 
            ON t.block_date = d.block_date
        --max daily block_number from 1 day ago, for each address
    WHERE
        t.block_date IS NOT NULL
        AND d.block_number = (
            SELECT MAX(block_number)
            FROM last_x_days
        )
    EXCEPT
    SELECT
        block_number,
        block_date,
        address
    FROM
        {{ ref("streamline__balances_native_complete_daily") }}
    WHERE
        block_number = (
            SELECT MAX(block_number)
            FROM last_x_days
        )
        AND block_number IS NOT NULL
        AND _inserted_timestamp :: DATE >= DATEADD(
            'day',
            -7,
            SYSDATE()
        )
)
SELECT
    block_number,
    DATE_PART('EPOCH_SECONDS', block_date) :: INT AS block_date_unix,
    address,
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

LIMIT {{ vars.BALANCES_SL_NATIVE_REALTIME_SQL_LIMIT }}

{# Streamline Function Call #}
{% if execute %}
    {% set params = {
        "external_table": 'balances_native',
        "sql_limit": vars.BALANCES_SL_NATIVE_REALTIME_SQL_LIMIT,
        "producer_batch_size": vars.BALANCES_SL_NATIVE_REALTIME_PRODUCER_BATCH_SIZE,
        "worker_batch_size": vars.BALANCES_SL_NATIVE_REALTIME_WORKER_BATCH_SIZE,
        "async_concurrent_requests": vars.BALANCES_SL_NATIVE_REALTIME_ASYNC_CONCURRENT_REQUESTS,
        "sql_source": 'balances_native_realtime_daily'
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