{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = "view",
    tags = ['streamline','balances','history','phase_4']
) }}

{# Main query starts here #}
WITH 
    last_3_days AS (
        SELECT block_number
        FROM {{ ref("_block_lookback") }}
    ),

{# Identify blocks that need processing #}
to_do AS (
    SELECT block_number
    FROM {{ ref("streamline__blocks") }}
    WHERE 
        block_number IS NOT NULL
        AND block_number <= (SELECT block_number FROM last_3_days)
    EXCEPT

    {# Exclude blocks that have already been processed #}
    SELECT block_number
    FROM {{ ref('streamline__state_tracer_complete') }}
    WHERE 1=1
        AND block_number <= (SELECT block_number FROM last_3_days)
)

{# Prepare the final list of blocks to process #}
,ready_blocks AS (
    SELECT block_number
    FROM to_do

    {% if vars.BALANCES_SL_TESTING_LIMIT is not none %}
        ORDER BY block_number DESC
        LIMIT {{ vars.BALANCES_SL_TESTING_LIMIT }} 
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
            'method', 'debug_traceBlockByNumber',
            'params', ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), 
                OBJECT_CONSTRUCT(
                'tracer', 'prestateTracer', 
                'tracerConfig', OBJECT_CONSTRUCT('diffMode', TRUE),
                'timeout', '120s'
                )
            )
        ),
        '{{ vars.GLOBAL_NODE_VAULT_PATH }}'
    ) AS request
FROM
    ready_blocks
    
ORDER BY partition_key DESC, block_number DESC

LIMIT {{ vars.BALANCES_SL_STATE_TRACER_HISTORY_SQL_LIMIT }}

{# Streamline Function Call #}
{% if execute %}
    {% set params = {
        "external_table": 'state_tracer',
        "sql_limit": vars.BALANCES_SL_STATE_TRACER_HISTORY_SQL_LIMIT,
        "producer_batch_size": vars.BALANCES_SL_STATE_TRACER_HISTORY_PRODUCER_BATCH_SIZE,
        "worker_batch_size": vars.BALANCES_SL_STATE_TRACER_HISTORY_WORKER_BATCH_SIZE,
        "async_concurrent_requests": vars.BALANCES_SL_STATE_TRACER_HISTORY_ASYNC_CONCURRENT_REQUESTS,
        "sql_source": 'state_tracer_history',
        "exploded_key": tojson(['result'])
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