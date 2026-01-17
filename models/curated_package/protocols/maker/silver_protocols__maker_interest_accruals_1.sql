{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['ts', 'hash', 'ilk'],
    cluster_by = ['ts'],
    tags = ['silver_protocols', 'maker', 'interest_accruals', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

SELECT
    ilk,
    block_timestamp AS ts,
    tx_hash AS hash,
    dart,
    CAST(NULL AS NUMBER) AS rate,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM {{ ref('maker__fact_vat_frob') }}
WHERE dart != 0
{% if is_incremental() %}
AND block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
{% endif %}

UNION ALL

SELECT
    ilk,
    block_timestamp AS ts,
    tx_hash AS hash,
    dart / 1e18,
    0 AS rate,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM {{ ref('silver_protocols__maker_vat_grab') }}
WHERE dart != 0
{% if is_incremental() %}
AND block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
{% endif %}

UNION ALL

SELECT
    ilk,
    block_timestamp AS ts,
    tx_hash AS hash,
    CAST(NULL AS NUMBER) AS dart,
    rate,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM {{ ref('maker__fact_vat_fold') }}
WHERE rate != 0
{% if is_incremental() %}
AND block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
{% endif %}
