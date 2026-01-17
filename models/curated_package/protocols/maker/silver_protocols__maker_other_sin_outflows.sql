{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['ts', 'hash', 'code'],
    cluster_by = ['ts'],
    tags = ['silver_protocols', 'maker', 'sin_outflows', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH other_sin_outflows_raw AS (
    SELECT
        block_timestamp AS ts,
        tx_hash AS hash,
        CAST(rad AS DOUBLE) AS value
    FROM {{ ref('maker__fact_vat_suck') }}
    WHERE u_address = '0xa950524441892a31ebddf91d3ceefa04bf454466'
      AND v_address NOT IN (
        '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7',
        '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb',
        '0x2cc583c0aacdac9e23cb601fda8f1a0c56cdcb71',
        '0xa4c22f0e25c6630b2017979acf1f865e94695c4b'
      )
    {% if is_incremental() %}
    AND block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
    {% endif %}
)

SELECT
    ts,
    hash,
    31520 AS code,
    -value AS value,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM other_sin_outflows_raw

UNION ALL

SELECT
    ts,
    hash,
    21120 AS code,
    value AS value,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM other_sin_outflows_raw
