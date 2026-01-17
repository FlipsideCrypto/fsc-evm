{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['ts', 'hash', 'code', 'ilk'],
    cluster_by = ['ts'],
    tags = ['silver_protocols', 'maker', 'psm_yield', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH psm_yield_preunioned AS (
    SELECT
        vat.block_timestamp AS ts,
        vat.tx_hash AS hash,
        tx.ilk,
        SUM(vat.rad) AS value
    FROM {{ ref('maker__fact_vat_move') }} vat
    INNER JOIN {{ ref('fact_psm_yield_tx') }} tx
        ON vat.tx_hash = tx.tx_hash
    WHERE vat.dst_address = '0xa950524441892a31ebddf91d3ceefa04bf454466'
    {% if is_incremental() %}
    AND vat.block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
    {% endif %}
    GROUP BY vat.block_timestamp, vat.tx_hash, tx.ilk
)

SELECT
    ts,
    hash,
    31180 AS code,
    value,
    ilk,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM psm_yield_preunioned

UNION ALL

SELECT
    ts,
    hash,
    21120 AS code,
    -value AS value,
    ilk,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM psm_yield_preunioned
