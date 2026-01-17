{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['ts', 'hash', 'code', 'ilk'],
    cluster_by = ['ts'],
    tags = ['silver_protocols', 'maker', 'd3m_revenues', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH d3m_revenues_preunion AS (
    SELECT
        block_timestamp AS ts,
        tx_hash AS hash,
        CASE
            WHEN src_address = '0xa13c0c8eb109f5a13c6c90fc26afb23beb3fb04a' THEN 'DIRECT-AAVEV2-DAI'
            WHEN src_address = '0x621fe4fde2617ea8ffade08d0ff5a862ad287ec2' THEN 'DIRECT-COMPV2-DAI'
        END AS ilk,
        SUM(CAST(rad AS DOUBLE)) AS value
    FROM {{ ref('maker__fact_vat_move') }}
    WHERE src_address IN (
        '0xa13c0c8eb109f5a13c6c90fc26afb23beb3fb04a',
        '0x621fe4fde2617ea8ffade08d0ff5a862ad287ec2'
    )
    AND dst_address = '0xa950524441892a31ebddf91d3ceefa04bf454466'
    {% if is_incremental() %}
    AND block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
    {% endif %}
    GROUP BY 1, 2, 3

    UNION ALL

    SELECT
        block_timestamp AS ts,
        tx_hash AS hash,
        ilk,
        SUM(dart) / 1e18 AS value
    FROM {{ ref('silver_protocols__maker_vat_grab') }}
    WHERE dart > 0
    {% if is_incremental() %}
    AND block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
    {% endif %}
    GROUP BY 1, 2, 3
)

SELECT
    ts,
    hash,
    31160 AS code,
    value AS value,
    ilk,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM d3m_revenues_preunion

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
FROM d3m_revenues_preunion
