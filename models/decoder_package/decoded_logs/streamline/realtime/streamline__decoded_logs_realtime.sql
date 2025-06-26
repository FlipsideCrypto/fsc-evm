{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = "view",
    tags = ['streamline','decoded_logs','realtime','phase_3']
) }}

WITH existing_logs_to_exclude AS (
    SELECT
        _log_id
    FROM
        {{ ref('streamline__decoded_logs_complete') }}
    WHERE
        inserted_timestamp >= DATEADD('hour', -60, SYSDATE())
),
candidate_logs AS (
    SELECT
        block_number,
        tx_hash,
        event_index,
        contract_address,
        topics,
        data,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id
    FROM {{ ref('core__fact_event_logs') }}
    WHERE
        inserted_timestamp >= DATEADD('hour', -48, SYSDATE())
        AND tx_succeeded
)
SELECT
    l.block_number,
    l._log_id,
    A.abi,
    OBJECT_CONSTRUCT(
        'topics',
        l.topics,
        'data',
        l.data,
        'address',
        l.contract_address
    ) AS DATA
FROM
    candidate_logs l
    INNER JOIN {{ source('abis_silver', 'complete_event_abis') }} A
    ON A.parent_contract_address = l.contract_address
    AND A.event_signature = l.topics [0] :: STRING
    AND l.block_number BETWEEN A.start_block
    AND A.end_block
WHERE
    NOT EXISTS (
        SELECT
            1
        FROM
            existing_logs_to_exclude e
        WHERE
            e._log_id = l._log_id
    )

{% if vars.DECODER_SL_TESTING_LIMIT is not none %}
    LIMIT
        {{ vars.DECODER_SL_TESTING_LIMIT }}
{% endif %}

{# Streamline Function Call #}
{% if execute %}
    {% set params = {
        "external_table": vars.DECODER_SL_DECODED_LOGS_REALTIME_EXTERNAL_TABLE,
        "sql_limit": vars.DECODER_SL_DECODED_LOGS_REALTIME_SQL_LIMIT,
        "producer_batch_size": vars.DECODER_SL_DECODED_LOGS_REALTIME_PRODUCER_BATCH_SIZE,
        "worker_batch_size": vars.DECODER_SL_DECODED_LOGS_REALTIME_WORKER_BATCH_SIZE,
        "sql_source": "decoded_logs_realtime"
    } %}

    {% set function_call_sql %}
    {{ fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_decode_logs_v2',
        target = this.schema ~ "." ~ this.identifier,
        params = params
    ) }}
    {% endset %}
    
    {% do run_query(function_call_sql) %}
    {{ log("Streamline function call: " ~ function_call_sql, info=true) }}
{% endif %}