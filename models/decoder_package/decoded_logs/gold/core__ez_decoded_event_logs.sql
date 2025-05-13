{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "incremental",
    unique_key = "ez_decoded_event_logs_id",
    incremental_strategy = 'delete+insert',
    cluster_by = "block_timestamp::date",
    incremental_predicates = [fsc_evm.standard_predicate()],
    full_refresh = vars.GLOBAL_GOLD_FR_ENABLED,
    post_hook = 'ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(ez_decoded_event_logs_id, contract_name, contract_address)',
    tags = ['gold','decoded_logs','phase_3']
) }}

WITH base AS (

    SELECT
        tx_hash,
        block_number,
        event_index,
        event_name,
        contract_address,
        decoded_data AS full_decoded_log,
        decoded_flat AS decoded_log
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        1 = 1

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        COALESCE(MAX(modified_timestamp), '2000-01-01' :: TIMESTAMP)
    FROM
        {{ this }})
    {% endif %}
),
new_records AS (
    SELECT
        b.block_number AS block_number,
        block_timestamp AS block_timestamp,
        b.tx_hash AS tx_hash,
        tx_position,
        b.event_index AS event_index,
        b.contract_address AS contract_address,
        topics,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        DATA,
        event_removed,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_succeeded,
        event_name,
        full_decoded_log,
        decoded_log,
        NAME AS contract_name
    FROM
        base b
        LEFT JOIN {{ ref('core__fact_event_logs') }}
        fel
        ON b.block_number = fel.block_number
        AND b.event_index = fel.event_index

{% if is_incremental() %}
AND fel.inserted_timestamp > DATEADD('day', -3, SYSDATE())
{% endif %}
LEFT JOIN {{ ref('core__dim_contracts') }}
dc
ON b.contract_address = dc.address
AND dc.name IS NOT NULL
WHERE
    1 = 1
)

{% if is_incremental() %},
missing_tx_data AS (
    SELECT
        t.block_number,
        fel.block_timestamp,
        t.tx_hash,
        fel.tx_position,
        t.event_index,
        t.contract_address,
        fel.topics,
        fel.topic_0,
        fel.topic_1,
        fel.topic_2,
        fel.topic_3,
        fel.data,
        fel.event_removed,
        fel.origin_from_address,
        fel.origin_to_address,
        fel.origin_function_signature,
        fel.tx_succeeded,
        t.event_name,
        t.full_decoded_log,
        t.decoded_log,
        t.contract_name
    FROM
        {{ this }}
        t
        INNER JOIN {{ ref('core__fact_event_logs') }}
        fel USING (
            block_number,
            event_index
        )
    WHERE
        t.tx_succeeded IS NULL
        OR t.block_timestamp IS NULL
        AND fel.block_timestamp IS NOT NULL
),
missing_contract_data AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        tx_position,
        event_index,
        contract_address,
        topics,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        DATA,
        event_removed,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_succeeded,
        event_name,
        full_decoded_log,
        decoded_log,
        dc.name AS contract_name
    FROM
        {{ this }}
        t
        INNER JOIN {{ ref('core__dim_contracts') }}
        dc
        ON t.contract_address = dc.address
        AND dc.name IS NOT NULL
    WHERE
        t.contract_name IS NULL
        and t.block_timestamp >= dateadd('day', -30, sysdate())
)
{% endif %},
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        tx_position,
        event_index,
        contract_address,
        topics,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        DATA,
        event_removed,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_succeeded,
        event_name,
        full_decoded_log,
        decoded_log,
        contract_name
    FROM
        new_records

{% if is_incremental() %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    tx_position,
    event_index,
    contract_address,
    topics,
    topic_0,
    topic_1,
    topic_2,
    topic_3,
    DATA,
    event_removed,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_succeeded,
    event_name,
    full_decoded_log,
    decoded_log,
    contract_name
FROM
    missing_tx_data
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    tx_position,
    event_index,
    contract_address,
    topics,
    topic_0,
    topic_1,
    topic_2,
    topic_3,
    DATA,
    event_removed,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_succeeded,
    event_name,
    full_decoded_log,
    decoded_log,
    contract_name
FROM
    missing_contract_data
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    tx_position,
    event_index,
    contract_address,
    topics,
    topic_0,
    topic_1,
    topic_2,
    topic_3,
    DATA,
    event_removed,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_succeeded,
    event_name,
    full_decoded_log,
    decoded_log,
    contract_name,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS ez_decoded_event_logs_id,
{% if is_incremental() or vars.GLOBAL_NEW_BUILD_ENABLED %}
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
{% else %}
    CASE WHEN block_timestamp >= date_trunc('hour',SYSDATE()) - interval '6 hours' THEN SYSDATE() 
        ELSE GREATEST(block_timestamp, dateadd('day', -10, SYSDATE())) END AS inserted_timestamp,
    CASE WHEN block_timestamp >= date_trunc('hour',SYSDATE()) - interval '6 hours' THEN SYSDATE() 
        ELSE GREATEST(block_timestamp, dateadd('day', -10, SYSDATE())) END AS modified_timestamp
{% endif %}
FROM
    FINAL qualify ROW_NUMBER() over (
        PARTITION BY ez_decoded_event_logs_id
        ORDER BY
            block_timestamp DESC nulls last,
            tx_succeeded DESC nulls last,
            contract_name DESC nulls last
    ) = 1
