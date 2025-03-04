{# Set variables #}
{%- set vars = return_vars() -%}
{%- set package_name = 'MAIN' -%}
{%- set model_name = 'BLOCKS_TRANSACTIONS' -%}
{%- set model_type = 'REALTIME' -%}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = {
            "external_table": model_name.lower(),
            "sql_limit": vars.MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_SQL_LIMIT,
            "producer_batch_size": vars.MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_PRODUCER_BATCH_SIZE,
            "worker_batch_size": vars.MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_WORKER_BATCH_SIZE,
            "sql_source": (model_name ~ '_' ~ model_type).lower(),
            "exploded_key": ['result', 'result.transactions']
        }
    ),
    tags = ['streamline_core_realtime']
) }}

{# Main query starts here #}
WITH 
{% if not vars.MAIN_SL_NEW_BUILD_ENABLED %}
    last_3_days AS (
        SELECT block_number
        FROM {{ ref("_block_lookback") }}
    ),
{% endif %}

{# Identify blocks that need processing #}
to_do AS (
    SELECT block_number
    FROM {{ ref("streamline__blocks") }}
    WHERE 
    block_number IS NOT NULL
    {% if not vars.MAIN_SL_NEW_BUILD_ENABLED %}
        AND block_number >= (SELECT block_number FROM last_3_days)
    {% endif %}

    {% if vars.MAIN_SL_MIN_BLOCK is not none %}
        AND block_number >= {{ vars.MAIN_SL_MIN_BLOCK }}
    {% endif %}

    EXCEPT

    SELECT block_number
    FROM {{ ref("streamline__blocks_complete") }} b
    INNER JOIN {{ ref("streamline__transactions_complete") }} t USING(block_number)
    WHERE 1=1
    {% if not vars.MAIN_SL_NEW_BUILD_ENABLED %}
        AND block_number >= (SELECT block_number FROM last_3_days)
    {% endif %}
),
ready_blocks AS (
    SELECT block_number
    FROM to_do

    {% if not vars.MAIN_SL_NEW_BUILD_ENABLED %}
        UNION
        SELECT block_number
        FROM {{ ref("_unconfirmed_blocks") }}
        UNION
        SELECT block_number
        FROM {{ ref("_missing_txs") }}
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
        '{{ vars.NODE_URL }}',
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
        '{{ vars.NODE_SECRET_PATH }}'
    ) AS request
FROM
    ready_blocks
    
ORDER BY partition_key DESC, block_number DESC

LIMIT {{ vars[package_name ~ '_SL_' ~ model_name ~ '_' ~ model_type ~ '_SQL_LIMIT'] }}