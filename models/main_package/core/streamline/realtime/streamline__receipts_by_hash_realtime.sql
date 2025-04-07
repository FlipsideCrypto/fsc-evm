{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = {
            "external_table": 'receipts_by_hash',
            "sql_limit": vars.MAIN_SL_RECEIPTS_BY_HASH_REALTIME_SQL_LIMIT,
            "producer_batch_size": vars.MAIN_SL_RECEIPTS_BY_HASH_REALTIME_PRODUCER_BATCH_SIZE,
            "worker_batch_size": vars.MAIN_SL_RECEIPTS_BY_HASH_REALTIME_WORKER_BATCH_SIZE,
            "async_concurrent_requests": vars.MAIN_SL_RECEIPTS_BY_HASH_REALTIME_ASYNC_CONCURRENT_REQUESTS,
            "sql_source": 'receipts_by_hash_realtime'
        }
    ),
    tags = ['streamline','core','realtime','receipts_by_hash']
) }}

{# Main query starts here #}
{# Start by invoking LQ for the last hour of blocks #}

WITH numbered_blocks AS (

    SELECT
        block_number_hex,
        block_number,
        ROW_NUMBER() over (
            ORDER BY
                block_number
        ) AS row_num
    FROM
        (
            SELECT
                *
            FROM
                {{ ref('streamline__blocks') }}
            ORDER BY
                block_number DESC
            LIMIT
                {{ vars.MAIN_SL_BLOCKS_PER_HOUR }}
        )
), batched_blocks AS (
    SELECT
        block_number_hex,
        block_number,
        100 AS rows_per_batch,
        CEIL(
            row_num / rows_per_batch
        ) AS batch_number,
        MOD(
            row_num - 1,
            rows_per_batch
        ) + 1 AS row_within_batch
    FROM
        numbered_blocks
),
batched_calls AS (
    SELECT
        batch_number,
        ARRAY_AGG(
            utils.udf_json_rpc_call(
                'eth_getBlockByNumber',
                [block_number_hex, false]
            )
        ) AS batch_request
    FROM
        batched_blocks
    GROUP BY
        batch_number
),
rpc_requests AS (
    SELECT
        live.udf_api(
            'POST',
            '{{ vars.GLOBAL_NODE_URL }}',
            OBJECT_CONSTRUCT(
                'Content-Type',
                'application/json',
                'fsc-quantum-state',
                'livequery'
            ),
            batch_request,
            '{{ vars.GLOBAL_NODE_VAULT_PATH }}'
        ) AS resp
    FROM
        batched_calls
),
blocks AS (
    SELECT
        utils.udf_hex_to_int(
            VALUE :result :number :: STRING
        ) :: INT AS block_number,
        VALUE :result :transactions AS tx_hashes
    FROM
        rpc_requests,
        LATERAL FLATTEN (
            input => resp :data
        )
),
flat_tx_hashes AS (
    SELECT
        block_number,
        VALUE :: STRING AS tx_hash
    FROM
        blocks,
        LATERAL FLATTEN (
            input => tx_hashes
        )
),
to_do AS (

    SELECT 
        block_number,
        tx_hash
    FROM (
        SELECT
            block_number,
            tx_hash
        FROM
            flat_tx_hashes
        WHERE 1=1
        {% if vars.MAIN_SL_MIN_BLOCK is not none %}
            AND block_number >= {{ vars.MAIN_SL_MIN_BLOCK }}
        {% endif %}

        {% if vars.MAIN_SL_RECEIPTS_BY_HASH_REALTIME_TXNS_MODEL_ENABLED %}
        UNION ALL
        SELECT
            block_number,
            tx_hash
        FROM
            {{ ref('test_gold__fact_transactions_recent') }}
        {% endif %}

    )

    EXCEPT

    SELECT
        block_number,
        tx_hash
    FROM
        {{ ref('streamline__receipts_by_hash_complete') }}
    WHERE 1=1
        {% if not vars.MAIN_SL_NEW_BUILD_ENABLED %}
            AND block_number >= (SELECT block_number FROM {{ ref('_block_lookback') }})
        {% endif %}
),
ready_blocks AS (
    SELECT
        block_number,
        tx_hash
    FROM
        to_do

    {% if not vars.MAIN_SL_NEW_BUILD_ENABLED %}

        UNION
        SELECT
            block_number,
            tx_hash
        FROM
            {{ ref('test_gold__fact_transactions_recent') }}
            JOIN {{ ref('_missing_receipts') }} using (block_number)

    {% endif %}

    {% if vars.MAIN_SL_TESTING_LIMIT is not none %}
        LIMIT {{ vars.MAIN_SL_TESTING_LIMIT }} 
    {% endif %}
)
SELECT
    block_number,
    tx_hash,
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
            'fsc-quantum-state', 'streamline'
        ),
        OBJECT_CONSTRUCT(
            'id', block_number,
            'jsonrpc', '2.0',
            'method', 'eth_getTransactionReceipt',
            'params', ARRAY_CONSTRUCT(tx_hash)
        ),
        '{{ vars.GLOBAL_NODE_VAULT_PATH }}'
    ) AS request
FROM
    ready_blocks

ORDER BY partition_key DESC, block_number DESC

LIMIT {{ vars.MAIN_SL_RECEIPTS_BY_HASH_REALTIME_SQL_LIMIT }}
