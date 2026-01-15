{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH sin_inflows_raw AS (
    SELECT 
        block_timestamp AS ts,
        tx_hash AS hash,
        CAST(rad AS DOUBLE) AS value
    FROM {{ ref('maker__fact_vat_suck') }}
    WHERE v_address = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- Vow
)

SELECT
    ts,
    hash,
    31510 AS code,
    value AS value --increased equity
FROM sin_inflows_raw

UNION ALL

SELECT
    ts,
    hash,
    21120 AS code,
    -value AS value --decreased liability
FROM sin_inflows_raw