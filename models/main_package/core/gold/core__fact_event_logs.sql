{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = vars.MAIN_CORE_GOLD_FACT_EVENT_LOGS_UNIQUE_KEY,
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(block_number,tx_hash,contract_address,origin_from_address,origin_to_address,origin_function_signature,topic_0)",
    incremental_predicates = [fsc_evm.standard_predicate()],
    full_refresh = vars.GLOBAL_GOLD_FR_ENABLED,
    tags = ['gold','core','phase_2']
) }}

WITH base AS (

    SELECT
        block_number,
        {% if vars.MAIN_CORE_RECEIPTS_BY_HASH_ENABLED %}
            tx_hash,
        {% else %}
            receipts_json :transactionHash :: STRING AS tx_hash,
            array_index,
        {% endif %}
        receipts_json :from :: STRING AS origin_from_address,
        receipts_json :to :: STRING AS origin_to_address,
        CASE
            WHEN receipts_json :status :: STRING = '0x1' THEN TRUE
            WHEN receipts_json :status :: STRING = '0x0' THEN FALSE
            ELSE NULL
        END AS tx_succeeded,
        receipts_json :logs AS full_logs
    FROM
        {{ ref('silver__receipts') }}
    WHERE
        ARRAY_SIZE(receipts_json :logs) > 0
        {% if is_incremental() %}
        AND modified_timestamp > (
            SELECT
                COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
            FROM
                {{ this }})
        {% endif %}
),
relevant_transactions AS (
    SELECT 
        block_number,
        tx_hash,
        block_timestamp,
        tx_position,
        origin_function_signature
    FROM {{ ref('core__fact_transactions') }}
    WHERE 1=1
    {% if is_incremental() %}
    AND block_timestamp >= (
        SELECT
            DATEADD('hour', -24, MAX(block_timestamp))
        FROM
            {{ this }}
    )
    {% endif %}
),
flattened_logs AS (
    SELECT
        b.block_number,
        b.tx_hash,
        b.origin_from_address,
        b.origin_to_address,
        b.tx_succeeded,
        {% if not vars.MAIN_CORE_RECEIPTS_BY_HASH_ENABLED %}
            b.array_index,
        {% endif %}
        VALUE :address :: STRING AS contract_address,
        VALUE :blockHash :: STRING AS block_hash,
        VALUE :blockNumber :: STRING AS block_number_hex,
        VALUE :data :: STRING AS DATA,
        utils.udf_hex_to_int(
            VALUE :logIndex :: STRING
        ) :: INT AS event_index,
        COALESCE(VALUE :removed :: BOOLEAN, FALSE) AS event_removed,
        VALUE :topics AS topics,
        topics [0] :: STRING AS topic_0,
        topics [1] :: STRING AS topic_1,
        topics [2] :: STRING AS topic_2,
        topics [3] :: STRING AS topic_3,
        VALUE :transactionHash :: STRING AS transaction_hash,
        utils.udf_hex_to_int(
            VALUE :transactionIndex :: STRING
        ) :: INT AS transaction_index
    FROM
        base b,
        LATERAL FLATTEN (
            input => full_logs
        )
),
materialized_logs AS (
    SELECT 
        block_number,
        tx_hash,
        origin_from_address,
        origin_to_address,
        tx_succeeded,
        {% if not vars.MAIN_CORE_RECEIPTS_BY_HASH_ENABLED %}
            array_index,
        {% endif %}
        contract_address,
        block_hash,
        block_number_hex,
        DATA,
        event_index,
        event_removed,
        topics,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        transaction_hash,
        transaction_index
    FROM flattened_logs
),
new_logs AS (
    SELECT
        l.block_number,
        txs.block_timestamp,
        l.tx_hash,
        COALESCE(txs.tx_position, l.transaction_index) AS tx_position,
        l.event_index,
        l.contract_address,
        l.topics,
        l.topic_0,
        l.topic_1,
        l.topic_2,
        l.topic_3,
        l.data,
        l.event_removed,
        l.origin_from_address,
        l.origin_to_address,
        txs.origin_function_signature,
        l.tx_succeeded
    FROM
        materialized_logs l
    LEFT JOIN relevant_transactions txs
        ON l.block_number = txs.block_number
        {% if vars.MAIN_CORE_RECEIPTS_BY_HASH_ENABLED %}
        AND l.tx_hash = txs.tx_hash
        {% else %}
        AND l.array_index = txs.tx_position
        {% endif %}
)

{% if is_incremental() %},
missing_data AS (
    SELECT
        t.block_number,
        txs.block_timestamp AS block_timestamp_heal,
        t.tx_hash,
        t.tx_position,
        t.event_index,
        t.contract_address,
        t.topics,
        t.topic_0,
        t.topic_1,
        t.topic_2,
        t.topic_3,
        t.data,
        t.event_removed,
        t.origin_from_address,
        t.origin_to_address,
        txs.origin_function_signature AS origin_function_signature_heal,
        t.tx_succeeded
    FROM
        {{ this }}
        t
        LEFT JOIN {{ ref('core__fact_transactions') }}
        txs
        ON t.tx_hash = txs.tx_hash
        AND t.block_number = txs.block_number
    WHERE
        t.block_timestamp IS NULL
        OR t.origin_function_signature IS NULL
)
{% endif %},
all_logs AS (
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
        tx_succeeded
    FROM
        new_logs

{% if is_incremental() %}
UNION ALL
SELECT
    block_number,
    block_timestamp_heal AS block_timestamp,
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
    origin_function_signature_heal AS origin_function_signature,
    tx_succeeded
FROM
    missing_data
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
    {{ dbt_utils.generate_surrogate_key(['tx_hash','event_index']) }} AS fact_event_logs_id,
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
    all_logs 
qualify ROW_NUMBER() over (
        PARTITION BY fact_event_logs_id
        ORDER BY
            block_number DESC,
            block_timestamp DESC nulls last,
            origin_function_signature DESC nulls last
    ) = 1