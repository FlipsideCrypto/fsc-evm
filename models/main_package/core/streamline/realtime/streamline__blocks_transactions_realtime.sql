{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = {
            "external_table": "blocks",
            "sql_limit": get_config_var('MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_SQL_LIMIT'),
            "producer_batch_size": get_config_var('MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_PRODUCER_BATCH_SIZE'),
            "worker_batch_size": get_config_var('MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_WORKER_BATCH_SIZE'),
            "async_concurrent_requests": get_config_var('MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_ASYNC_CONCURRENT_REQUESTS'),
            "exploded_key": ['result', 'result.transactions'],
            "sql_source" :"{{this.identifier}}"
        }
    ),
    tags = ['streamline_core_realtime', 'return_vars2']
) }}

{# Main query starts here #}
WITH 
{# Identify blocks that need processing #}
to_do AS (
    SELECT block_number
    FROM {{ ref("streamline__blocks") }}
    WHERE 
    block_number IS NOT NULL
    {% if not get_config_var('MAIN_SL_NEW_BUILD_ENABLED') %}
        AND block_number >= (SELECT block_number FROM {{ ref("_block_lookback") }})
    {% endif %}

    EXCEPT

    SELECT block_number
    FROM {{ ref("streamline__blocks_complete") }} b
    INNER JOIN {{ ref("streamline__transactions_complete") }} t USING(block_number)
    WHERE 1=1
    {% if not get_config_var('MAIN_SL_NEW_BUILD_ENABLED') %}
        AND block_number >= (SELECT block_number FROM {{ ref("_block_lookback") }})
    {% endif %}
),
ready_blocks AS (
    SELECT block_number
    FROM to_do

    {% if not get_config_var('MAIN_SL_NEW_BUILD_ENABLED') %}
        UNION
        SELECT block_number
        FROM {{ ref("_unconfirmed_blocks") }}
        UNION
        SELECT block_number
        FROM {{ ref("_missing_txs") }}
    {% endif %}

    {% if get_config_var('MAIN_SL_TESTING_LIMIT') is not none %}
        LIMIT {{ get_config_var('MAIN_SL_TESTING_LIMIT') }} 
    {% endif %}
)

{# Generate API requests for each block #}
SELECT
    block_number,
    ROUND(block_number, -3) AS partition_key,
    live.udf_api(
        'POST',
        get_config_var('GLOBAL_NODE_URL'),
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
        get_config_var('GLOBAL_NODE_SECRET_PATH')
    ) AS request
FROM
    ready_blocks
    
ORDER BY block_number DESC

LIMIT {{ get_config_var('MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_SQL_LIMIT') }}