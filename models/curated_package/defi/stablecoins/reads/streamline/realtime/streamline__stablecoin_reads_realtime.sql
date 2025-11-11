{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = "view",
    tags = ['streamline','stablecoin_reads','realtime','phase_4']
) }}

WITH verified_stablecoins AS (
    SELECT
        contract_address
    FROM
        {{ ref('defi__dim_stablecoins') }}
    WHERE
        is_verified
        AND contract_address IS NOT NULL
),
max_blocks AS (
    SELECT
        block_number,
        block_date
    FROM
        {{ ref("_max_block_by_date") }}
    WHERE 
        block_date >= DATEADD('day',-4,SYSDATE()) --last 3 max block_number by date
),
base AS (
    SELECT
        s.contract_address,
        m.block_number AS latest_block,
        m.block_date
    FROM
        verified_stablecoins s
        CROSS JOIN max_blocks m
        LEFT JOIN {{ ref('streamline__stablecoin_reads_complete') }} c
        ON s.contract_address = c.contract_address
        AND m.block_number = c.block_number
    WHERE
        c.contract_address IS NULL
),
function_sigs AS (
    SELECT
        '0x18160ddd' AS function_sig,
        'totalSupply' AS function_name
),
ready_reads AS (
    SELECT
        contract_address,
        latest_block,
        block_date,
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
    DATE_PART('EPOCH_SECONDS', block_date) :: INT AS block_date_unix,
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
        "external_table" :"stablecoin_reads",
        "sql_limit" : vars.CURATED_SL_STABLECOIN_READS_REALTIME_SQL_LIMIT,
        "producer_batch_size" : vars.CURATED_SL_STABLECOIN_READS_REALTIME_PRODUCER_BATCH_SIZE,
        "worker_batch_size" : vars.CURATED_SL_STABLECOIN_READS_REALTIME_WORKER_BATCH_SIZE,
        "async_concurrent_requests" : vars.CURATED_SL_STABLECOIN_READS_REALTIME_ASYNC_CONCURRENT_REQUESTS,
        "sql_source" : 'stablecoin_reads_realtime'
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

