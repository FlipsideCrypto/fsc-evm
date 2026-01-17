{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_timestamp', 'tx_hash'],
    cluster_by = ['block_timestamp'],
    tags = ['silver_protocols', 'maker', 'vow_fess', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

SELECT
    block_timestamp,
    utils.udf_hex_to_int(topics[2])::DOUBLE / 1e45 AS tab,
    tx_hash,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM {{ ref('core__fact_event_logs') }}
WHERE topics[0] = '0x697efb7800000000000000000000000000000000000000000000000000000000'
AND contract_address = LOWER('0xA950524441892A31ebddF91d3cEEFa04Bf454466')
{% if is_incremental() %}
AND block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
{% endif %}
