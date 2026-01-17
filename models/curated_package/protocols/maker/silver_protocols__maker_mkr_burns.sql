{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['ts', 'hash', 'code'],
    cluster_by = ['ts'],
    tags = ['silver_protocols', 'maker', 'mkr_burns', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH mkr_burns_preunioned AS (
    SELECT
        block_timestamp AS ts,
        tx_hash AS hash,
        SUM(CAST(rad AS DOUBLE)) AS value
    FROM {{ ref('maker__fact_vat_move') }}
    WHERE src_address = '0xa950524441892a31ebddf91d3ceefa04bf454466'
    {% if is_incremental() %}
    AND block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
    {% endif %}
    GROUP BY block_timestamp, tx_hash
)

SELECT
    ts,
    hash,
    31420 AS code,
    -value AS value,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM mkr_burns_preunioned

UNION ALL

SELECT
    ts,
    hash,
    21120 AS code,
    value,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM mkr_burns_preunioned
