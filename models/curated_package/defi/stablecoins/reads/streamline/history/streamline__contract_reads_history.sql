{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = "view",
    tags = ['streamline','stablecoin_reads','history','phase_4']
) }}

WITH verified_stablecoins AS (
    SELECT
        contract_address,
        OBJECT_CONSTRUCT(
            'symbol', symbol,
            'name', name,
            'label', label,
            'decimals', decimals,
            'is_verified', is_verified
        ) :: VARIANT AS metadata
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
        block_date >= ('{{ vars.BALANCES_SL_START_DATE }}' :: TIMESTAMP) :: DATE
),
base AS (
    SELECT
        s.contract_address,
        s.metadata,
        m.block_number,
        m.block_date
    FROM
        verified_stablecoins s
        CROSS JOIN max_blocks m
        LEFT JOIN {{ ref('streamline__stablecoin_reads_complete') }} c
        ON s.contract_address = c.contract_address
        AND m.block_number = c.block_number
    WHERE
        c.contract_address IS NULL
        AND m.block_date < (
            SELECT MAX(block_date)
            FROM max_blocks
        )
),
function_sigs AS (
    SELECT
        '0x18160ddd' AS function_sig,
        'totalSupply' AS function_name
),
ready_reads AS (
    SELECT
        contract_address,
        block_number,
        block_date,
        function_sig,
        RPAD(
            function_sig,
            64,
            '0'
        ) AS input,
        metadata
    FROM
        base
        JOIN function_sigs
        ON 1 = 1
)
SELECT
    contract_address,
    block_number,
    DATE_PART('EPOCH_SECONDS', block_date) :: INT AS block_date_unix,
    ROUND(block_number,-3) AS partition_key,
    function_sig,
    input,
    metadata :: STRING AS metadata_str,
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
            'params', [{'to': contract_address, 'from': null, 'data': input}, utils.udf_int_to_hex(block_number)],
            'id', concat_ws(
                '-',
                contract_address,
                input,
                block_number
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
        "external_table": 'contract_reads',
        "sql_limit": vars.CURATED_SL_STABLECOIN_READS_HISTORY_SQL_LIMIT,
        "producer_batch_size": vars.CURATED_SL_STABLECOIN_READS_HISTORY_PRODUCER_BATCH_SIZE,
        "worker_batch_size": vars.CURATED_SL_STABLECOIN_READS_HISTORY_WORKER_BATCH_SIZE,
        "async_concurrent_requests": vars.CURATED_SL_STABLECOIN_READS_HISTORY_ASYNC_CONCURRENT_REQUESTS,
        "sql_source": 'contract_reads_history'
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

