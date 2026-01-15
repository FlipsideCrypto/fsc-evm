{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH
  deltas AS (
    SELECT
      '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7' AS wallet,
      DATE(block_timestamp) AS dt,
      CAST(rad AS DOUBLE) AS delta,
    FROM
      {{ ref('maker__fact_VAT_move') }}
    WHERE dst_address = '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7'
    UNION ALL
    SELECT
      '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7' AS wallet,
      DATE(block_timestamp) AS dt,
      - CAST(rad AS DOUBLE) AS delta
    FROM
      {{ ref('maker__fact_VAT_move') }}
    WHERE src_address = '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7'
    UNION ALL
    SELECT
      '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7' AS wallet,
        DATE(block_timestamp) as dt,
      CAST(rad AS DOUBLE) AS delta
    FROM
      {{ ref('maker__fact_VAT_suck') }}
    WHERE v_address = '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7'
)
, daily_supply as (
    SELECT
        dt,
        sum(delta) as dai_supply
    FROM deltas
    GROUP BY 1
)
SELECT
    dt as date,
    SUM(dai_supply) OVER (ORDER BY dt) as dai_supply,
    'Ethereum' as chain
FROM daily_supply