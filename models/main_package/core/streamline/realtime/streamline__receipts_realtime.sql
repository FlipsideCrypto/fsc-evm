{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = {
            "external_table": "receipts",
            "sql_limit": {{MAIN_SL_RECEIPTS_REALTIME_SQL_LIMIT}},
            "producer_batch_size": {{MAIN_SL_RECEIPTS_REALTIME_PRODUCER_BATCH_SIZE}},
            "worker_batch_size": {{MAIN_SL_RECEIPTS_REALTIME_WORKER_BATCH_SIZE}},
            "async_concurrent_requests": {{MAIN_SL_RECEIPTS_REALTIME_ASYNC_CONCURRENT_REQUESTS}},
            "exploded_key": ['result'],
            "sql_source" :"{{this.identifier}}"
        }
    ),
    tags = ['streamline_core_realtime_receipts']
) }}

{# Main query starts here #}
WITH 
{# Identify blocks that need processing #}
to_do AS (
    SELECT block_number
    FROM {{ ref("streamline__blocks") }}
    WHERE 
        block_number IS NOT NULL
    {% if not MAIN_SL_NEW_BUILD_ENABLED %}
        AND block_number >= (SELECT block_number FROM {{ ref("_block_lookback") }})
    {% endif %}

    EXCEPT

    {# Exclude blocks that have already been processed #}
    SELECT block_number
    FROM {{ ref('streamline__receipts_complete') }}
    WHERE 1=1
    {% if not MAIN_SL_NEW_BUILD_ENABLED %}
        AND block_number >= (SELECT block_number FROM {{ ref("_block_lookback") }})
    {% endif %}
)

{# Prepare the final list of blocks to process #}
,ready_blocks AS (
    SELECT block_number
    FROM to_do

    {% if not MAIN_SL_NEW_BUILD_ENABLED %}
        UNION
        SELECT block_number
        FROM {{ ref("_unconfirmed_blocks") }}
        UNION
        SELECT block_number
        FROM {{ ref("_missing_txs") }}
        UNION
        SELECT block_number
        FROM {{ ref("_missing_receipts") }}
    {% endif %}

    {% if MAIN_SL_TESTING_LIMIT is not none %}
        LIMIT {{ MAIN_SL_TESTING_LIMIT }} 
    {% endif %}
)

{# Generate API requests for each block #}
SELECT
    block_number,
    ROUND(block_number, -3) AS partition_key,
    live.udf_api(
        'POST',
        '{{ node_url }}',
        OBJECT_CONSTRUCT(
            'Content-Type', 'application/json',
            'fsc-quantum-state', 'streamline'
        ),
        OBJECT_CONSTRUCT(
            'id', block_number,
            'jsonrpc', '2.0',
            'method', 'eth_getBlockReceipts',
            'params', ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number))
        ),
        '{{ node_secret_path }}'
    ) AS request
FROM
    ready_blocks
    
ORDER BY block_number DESC

LIMIT {{ MAIN_SL_RECEIPTS_REALTIME_SQL_LIMIT }}