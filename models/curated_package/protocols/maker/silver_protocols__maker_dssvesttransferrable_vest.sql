{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_timestamp', 'tx_hash', '_id'],
    cluster_by = ['block_timestamp'],
    tags = ['silver_protocols', 'maker', 'dssvest', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

SELECT
    block_timestamp,
    tx_hash,
    utils.udf_hex_to_int(topics[1]) AS _id,
    utils.udf_hex_to_int(data) AS _max_amt,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM {{ ref('core__fact_event_logs') }}
WHERE contract_address = LOWER('0x6D635c8d08a1eA2F1687a5E46b666949c977B7dd')
AND topics[0] = '0xa2906882572b0e9dfe893158bb064bc308eb1bd87d1da481850f9d17fc293847'
{% if is_incremental() %}
AND block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
{% endif %}
