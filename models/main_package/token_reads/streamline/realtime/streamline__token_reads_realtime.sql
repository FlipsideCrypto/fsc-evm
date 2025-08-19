{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = "view",
    tags = ['streamline','token_reads','realtime','phase_2']
) }}

WITH base AS (

    SELECT
        r.contract_address,
        r.latest_event_block AS latest_block,
        r.total_event_count
    FROM
        {{ ref('silver__relevant_contracts') }} r 
        LEFT JOIN {{ ref('streamline__token_reads_complete') }} c
        USING (contract_address)
    WHERE
        c.contract_address IS NULL
        AND r.total_event_count >= 25
        AND r.latest_event_block > (
            SELECT max(block_number) 
            FROM {{ ref('core__fact_blocks') }} 
            WHERE block_timestamp::date = dateadd('day',-60,sysdate())::Date
        )
    UNION ALL
    select 
        contract_address,
        latest_event_block,
        total_event_count
    FROM {{ ref('_missing_token_reads') }}
    ORDER BY
        total_event_count DESC
    LIMIT {{ vars.MAIN_SL_TOKEN_READS_CONTRACT_LIMIT }}
), 
function_sigs AS (
    SELECT
        '0x313ce567' AS function_sig,
        'decimals' AS function_name
    UNION
    SELECT
        '0x06fdde03',
        'name'
    UNION
    SELECT
        '0x95d89b41',
        'symbol'
),
ready_reads AS (
    SELECT
        contract_address,
        latest_block,
        function_sig,
        RPAD(
            function_sig,
            64,
            '0'
        ) AS input
    FROM
        base
        JOIN function_sigs
        ON 1 = 1
)
SELECT
    contract_address,
    latest_block,
    ROUND(latest_block,-3) AS partition_key,
    function_sig,
    input,
    live.udf_api(
        'POST',
        '{{ vars.GLOBAL_NODE_URL }}',
        OBJECT_CONSTRUCT(
        'Content-Type', 'application/json',
        'fsc-quantum-state', 'streamline'
        ),
        OBJECT_CONSTRUCT(
            'method', 'eth_call',
            'jsonrpc', '2.0',
            'params', [{'to': contract_address, 'from': null, 'data': input}, utils.udf_int_to_hex(latest_block)],
            'id', concat_ws(
                '-',
                contract_address,
                input,
                latest_block
            )
        ),
        '{{ vars.GLOBAL_NODE_VAULT_PATH }}'
    ) AS request
FROM
    ready_reads
WHERE
    EXISTS (
        SELECT
            1
        FROM
            ready_reads
        LIMIT
            1
    )

{# Streamline Function Call #}
{% if execute %}
    {% set params = { 
        "external_table" :"token_reads",
        "sql_limit" : vars.MAIN_SL_TOKEN_READS_REALTIME_SQL_LIMIT,
        "producer_batch_size" : vars.MAIN_SL_TOKEN_READS_REALTIME_PRODUCER_BATCH_SIZE,
        "worker_batch_size" : vars.MAIN_SL_TOKEN_READS_REALTIME_WORKER_BATCH_SIZE,
        "async_concurrent_requests" : vars.MAIN_SL_TOKEN_READS_REALTIME_ASYNC_CONCURRENT_REQUESTS,
        "sql_source" : 'token_reads_realtime'
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