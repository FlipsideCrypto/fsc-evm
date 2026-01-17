{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'chain'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'maker', 'dai_dsr_supply', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH deltas AS (
    SELECT
        '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7' AS wallet,
        DATE(block_timestamp) AS dt,
        CAST(rad AS DOUBLE) AS delta
    FROM
        {{ ref('maker__fact_VAT_move') }}
    WHERE dst_address = '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7'
    {% if is_incremental() %}
    AND block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
    {% endif %}

    UNION ALL

    SELECT
        '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7' AS wallet,
        DATE(block_timestamp) AS dt,
        -CAST(rad AS DOUBLE) AS delta
    FROM
        {{ ref('maker__fact_VAT_move') }}
    WHERE src_address = '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7'
    {% if is_incremental() %}
    AND block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
    {% endif %}

    UNION ALL

    SELECT
        '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7' AS wallet,
        DATE(block_timestamp) AS dt,
        CAST(rad AS DOUBLE) AS delta
    FROM
        {{ ref('maker__fact_VAT_suck') }}
    WHERE v_address = '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7'
    {% if is_incremental() %}
    AND block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
    {% endif %}
),

daily_supply AS (
    SELECT
        dt,
        SUM(delta) AS dai_supply
    FROM deltas
    GROUP BY 1
)

SELECT
    dt AS date,
    SUM(dai_supply) OVER (ORDER BY dt) AS dai_supply,
    'Ethereum' AS chain,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM daily_supply
