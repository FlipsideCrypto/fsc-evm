{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['ts', 'hash', 'code'],
    cluster_by = ['ts'],
    tags = ['silver_protocols', 'maker', 'mkr_mints', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH mkr_mints_preunioned AS (
    SELECT
        vat.block_timestamp AS ts,
        vat.tx_hash AS hash,
        SUM(vat.rad) AS value
    FROM {{ ref('maker__fact_vat_move') }} vat
    JOIN {{ ref('silver_protocols__maker_liquidation_excluded_tx') }} tx
        ON vat.tx_hash = tx.tx_hash
    WHERE vat.dst_address = '0xa950524441892a31ebddf91d3ceefa04bf454466'
      AND vat.src_address NOT IN (SELECT contract_address FROM {{ ref('dim_maker_contracts') }} WHERE contract_type = 'PSM')
    {% if is_incremental() %}
    AND vat.block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
    {% endif %}
    GROUP BY vat.block_timestamp, vat.tx_hash
)

SELECT
    ts,
    hash,
    31410 AS code,
    value,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM mkr_mints_preunioned

UNION ALL

SELECT
    ts,
    hash,
    21120 AS code,
    -value AS value,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM mkr_mints_preunioned
