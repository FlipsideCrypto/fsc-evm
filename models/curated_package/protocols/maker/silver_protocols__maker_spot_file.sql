{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_timestamp', 'tx_hash', 'ilk'],
    cluster_by = ['block_timestamp'],
    tags = ['silver_protocols', 'maker', 'spot_file', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

SELECT
    block_timestamp,
    utils.udf_hex_to_string(RTRIM(topics[2], 0)) AS ilk,
    tx_hash,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM {{ ref('core__fact_event_logs') }}
WHERE topics[0] = '0x1a0b287e00000000000000000000000000000000000000000000000000000000'
AND contract_address ILIKE '0x65C79fcB50Ca1594B025960e539eD7A9a6D434A3'
{% if is_incremental() %}
AND block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
{% endif %}
