{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "intent_id",
    tags = ['silver_bridge','defi','bridge','curated']
) }}

WITH new_intents AS (

    SELECT
        DISTINCT intent_id
    FROM
        {{ ref('silver_bridge__everclear_intent_added') }}
    WHERE
        destination_count > 1

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ var("LOOKBACK", "12 hours") }}'
    FROM
        {{ this }}
)
AND intent_id NOT IN (
    SELECT
        intent_id
    FROM
        {{ this }}
    WHERE
        status = 'SETTLED_AND_COMPLETED'
)
{% endif %}
)

{% if is_incremental() %},
incomplete_txs AS (
    SELECT
        intent_id
    FROM
        {{ this }}
    WHERE
        status != 'SETTLED_AND_COMPLETED'
        AND modified_timestamp >= CURRENT_DATE() - INTERVAL '10 days'
)
{% endif %},
all_requests AS (
    SELECT
        intent_id
    FROM
        new_intents

{% if is_incremental() %}
UNION
SELECT
    intent_id
FROM
    incomplete_txs
{% endif %}
)
SELECT
    intent_id,
    live.udf_api(
        CONCAT(
            'https://api.everclear.org/intents/',
            intent_id
        )
    ) AS response,
    LOWER(
        response :data :intent :output_asset :: STRING
    ) AS output_asset,
    response :data :intent :status :: STRING AS status,
    response :data :intent :hub_settlement_domain :: STRING AS destination_chain_id
FROM
    all_requests
