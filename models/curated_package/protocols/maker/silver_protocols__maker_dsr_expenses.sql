{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['ts', 'hash', 'code'],
    cluster_by = ['ts'],
    tags = ['silver_protocols', 'maker', 'dsr_expenses', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH dsr_expenses_raw AS (
    SELECT
        block_timestamp AS ts,
        tx_hash AS hash,
        CAST(rad AS DOUBLE) AS value
    FROM {{ ref('maker__fact_vat_suck') }}
    WHERE u_address = '0xa950524441892a31ebddf91d3ceefa04bf454466'
      AND v_address = '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7'
    {% if is_incremental() %}
    AND block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
    {% endif %}
)

SELECT
    ts,
    hash,
    31610 AS code,
    -value AS value,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM dsr_expenses_raw

UNION ALL

SELECT
    ts,
    hash,
    21110 AS code,
    value AS value,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM dsr_expenses_raw
