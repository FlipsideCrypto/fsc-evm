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
            "external_table": 'confirm_blocks',
            "sql_limit": vars.MAIN_SL_CONFIRM_BLOCKS_HISTORY_SQL_LIMIT,
            "producer_batch_size": vars.MAIN_SL_CONFIRM_BLOCKS_HISTORY_PRODUCER_BATCH_SIZE,
            "worker_batch_size": vars.MAIN_SL_CONFIRM_BLOCKS_HISTORY_WORKER_BATCH_SIZE,
            "async_concurrent_requests": vars.MAIN_SL_CONFIRM_BLOCKS_HISTORY_ASYNC_CONCURRENT_REQUESTS,
            "sql_source": 'confirm_blocks_history'
        }
    ),
    tags = ['silver','streamline','core','history','confirm_blocks']
) }}

{# Main query starts here #}
WITH 
{% if not vars.MAIN_SL_NEW_BUILD_ENABLED %}
    last_3_days AS (
        SELECT block_number
        FROM {{ ref("_block_lookback") }}
    ),
{% endif %}

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
    {% if not vars.MAIN_SL_NEW_BUILD_ENABLED %}
        AND block_number <= (SELECT block_number FROM last_3_days)
    {% endif %}

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
    {% if not vars.MAIN_SL_NEW_BUILD_ENABLED %}
        AND block_number <= (SELECT block_number FROM last_3_days)
    {% endif %}
)

{# Prepare the final list of blocks to process #}
,ready_blocks AS (
    SELECT block_number
    FROM to_do

    {% if vars.MAIN_SL_MIN_BLOCK is not none %}
        WHERE block_number >= {{ vars.MAIN_SL_MIN_BLOCK }}
    {% endif %}

    {% if vars.MAIN_SL_TESTING_LIMIT is not none %}
        LIMIT {{ vars.MAIN_SL_TESTING_LIMIT }} 
    {% endif %}
)

{# Generate API requests for each block #}
SELECT
    block_number,
    ROUND(block_number, -3) AS partition_key,
    live.udf_api(
        'POST',
        '{{ vars.GLOBAL_NODE_URL }}',
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
        '{{ vars.GLOBAL_NODE_VAULT_PATH }}'
    ) AS request
FROM
    ready_blocks
    
ORDER BY partition_key DESC, block_number DESC

LIMIT {{ vars.MAIN_SL_CONFIRM_BLOCKS_HISTORY_SQL_LIMIT }}