{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['ts', 'hash', 'address'],
    cluster_by = ['ts'],
    tags = ['silver_protocols', 'maker', 'pause_proxy', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

SELECT
    block_timestamp AS ts,
    tx_hash AS hash,
    CAST(raw_amount_precise AS DOUBLE) AS expense,
    to_address AS address,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM {{ ref('core__ez_token_transfers') }}
WHERE contract_address = '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2'
  AND from_address = '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb'
  AND to_address != '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb'
{% if is_incremental() %}
  AND block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
{% endif %}

UNION ALL

SELECT
    block_timestamp AS ts,
    tx_hash AS hash,
    -CAST(raw_amount_precise AS DOUBLE) AS expense,
    from_address AS address,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM {{ ref('core__ez_token_transfers') }}
WHERE contract_address = '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2'
  AND to_address = '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb'
  AND from_address NOT IN ('0x8ee7d9235e01e6b42345120b5d270bdb763624c7', '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb')
{% if is_incremental() %}
  AND block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
{% endif %}
