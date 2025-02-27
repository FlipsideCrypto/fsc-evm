{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = {
            "external_table": "confirm_blocks",
            "sql_limit": {{MAIN_SL_CONFIRM_BLOCKS_REALTIME_SQL_LIMIT}},
            "producer_batch_size": {{MAIN_SL_CONFIRM_BLOCKS_REALTIME_PRODUCER_BATCH_SIZE}},
            "worker_batch_size": {{MAIN_SL_CONFIRM_BLOCKS_REALTIME_WORKER_BATCH_SIZE}},
            "async_concurrent_requests": {{MAIN_SL_CONFIRM_BLOCKS_REALTIME_ASYNC_CONCURRENT_REQUESTS}},
            "sql_source" :"{{this.identifier}}"
        }
    ),
    tags = ['streamline_core_realtime_confirm_blocks']
) }}

{# Main query starts here #}
WITH 
{# Delay blocks #}
look_back AS (
    SELECT
        block_number
    FROM
        {{ ref("_max_block_by_hour") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_number DESC
        ) = 6
    ),

{# Identify blocks that need processing #}
to_do AS (
    SELECT block_number
    FROM {{ ref("streamline__blocks") }}
    WHERE 
        block_number IS NOT NULL
        AND block_number <= (SELECT block_number FROM look_back)
        AND block_number >= (SELECT block_number FROM {{ ref("_block_lookback")}})

    EXCEPT

    {# Exclude blocks that have already been processed #}
    SELECT block_number
    FROM {{ ref('streamline__confirm_blocks_complete') }}
    WHERE 1=1
        AND block_number IS NOT NULL
        AND block_number <= (SELECT block_number FROM look_back)
        AND _inserted_timestamp >= DATEADD(
            'day',
            -4,
            SYSDATE()
        )
        AND block_number >= (SELECT block_number FROM {{ ref("_block_lookback")}})
)

{# Prepare the final list of blocks to process #}
,ready_blocks AS (
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
            'params', ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), FALSE)
        ),
        '{{ node_secret_path }}'
    ) AS request
FROM
    ready_blocks
    
ORDER BY block_number DESC

LIMIT {{ MAIN_SL_CONFIRM_BLOCKS_REALTIME_SQL_LIMIT }}