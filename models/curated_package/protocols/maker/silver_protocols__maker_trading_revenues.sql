{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['ts', 'hash', 'code', 'ilk'],
    cluster_by = ['ts'],
    tags = ['silver_protocols', 'maker', 'trading_revenues', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH trading_revenues_preunion AS (
    SELECT
        vat.block_timestamp AS ts,
        vat.tx_hash AS hash,
        psms.ilk,
        SUM(CAST(vat.rad AS DOUBLE)) AS value
    FROM {{ ref('maker__fact_vat_move') }} vat
    INNER JOIN {{ ref('silver_protocols__maker_psms') }} psms
        ON vat.src_address = psms.psm_address
    WHERE vat.dst_address = '0xa950524441892a31ebddf91d3ceefa04bf454466'
    {% if is_incremental() %}
    AND vat.block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
    {% endif %}
    GROUP BY vat.block_timestamp, vat.tx_hash, psms.ilk
)

SELECT
    ts,
    hash,
    31310 AS code,
    value AS value,
    ilk,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM trading_revenues_preunion

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
FROM trading_revenues_preunion
