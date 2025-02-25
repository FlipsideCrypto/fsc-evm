{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = {
            "external_table": "blocks",
            "sql_limit": {{MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_SQL_LIMIT}},
            "producer_batch_size": {{MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_PRODUCER_BATCH_SIZE}},
            "worker_batch_size": {{MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_WORKER_BATCH_SIZE}},
            "async_concurrent_requests": {{MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_ASYNC_CONCURRENT_REQUESTS}},
            "exploded_key": ['result', 'result.transactions'],
            "sql_source" :"{{this.identifier}}"
        }
    ),
    tags = ['streamline_core_history']
) }}

{# Main query starts here #}
WITH 
to_do AS (
    SELECT block_number
    FROM {{ ref("streamline__blocks") }}
    WHERE 
    block_number IS NOT NULL
    {% if not MAIN_SL_NEW_BUILD_ENABLED %}
        AND block_number <= (SELECT block_number FROM {{ ref("_block_lookback") }})
    {% endif %}

    EXCEPT

    SELECT block_number
    FROM {{ ref("streamline__blocks_complete") }} b
    INNER JOIN {{ ref("streamline__transactions_complete") }} t USING(block_number)
    WHERE 1=1
    {% if not MAIN_SL_NEW_BUILD_ENABLED %}
        AND block_number <= (SELECT block_number FROM {{ ref("_block_lookback") }})
    {% endif %}
),
ready_blocks AS (
    SELECT block_number
    FROM to_do

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
            'method', 'eth_getBlockByNumber',
            'params', ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), TRUE)
        ),
        '{{ node_secret_path }}'
    ) AS request
FROM
    ready_blocks
    
ORDER BY block_number DESC

LIMIT {{ MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_SQL_LIMIT }}