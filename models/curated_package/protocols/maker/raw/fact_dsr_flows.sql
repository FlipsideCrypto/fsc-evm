{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH dsr_flows_preunioned AS (
    SELECT 
        block_timestamp AS ts,
        tx_hash AS hash,
        -CAST(rad AS DOUBLE) AS dsr_flow
    FROM {{ ref('maker__fact_vat_move') }}
    WHERE src_address = '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7' -- Pot (DSR) contract

    UNION ALL

    SELECT 
        block_timestamp AS ts,
        tx_hash AS hash,
        CAST(rad AS DOUBLE) AS dsr_flow
    FROM {{ ref('maker__fact_vat_move') }}
    WHERE dst_address = '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7' -- Pot (DSR) contract
)

SELECT
    ts,
    hash,
    21110 AS code,
    dsr_flow AS value -- positive dsr flow increases interest-bearing dai liability
FROM dsr_flows_preunioned

UNION ALL

SELECT
    ts,
    hash,
    21120 AS code,
    -dsr_flow AS value -- positive dsr flow decreases non-interest-bearing dai liability
FROM dsr_flows_preunioned