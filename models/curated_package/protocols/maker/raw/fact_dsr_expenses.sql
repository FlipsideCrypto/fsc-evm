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
    WHERE u_address = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- Vow
      AND v_address = '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7' -- Pot
)

SELECT
    ts,
    hash,
    31610 AS code,
    -value AS value --reduced equity
FROM dsr_expenses_raw

UNION ALL

SELECT
    ts,
    hash,
    21110 AS code,
    value AS value --increased liability
FROM dsr_expenses_raw