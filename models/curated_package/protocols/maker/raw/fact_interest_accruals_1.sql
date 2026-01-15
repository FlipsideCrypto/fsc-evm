{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

SELECT 
    ilk,
    block_timestamp AS ts,
    tx_hash AS hash,
    dart,
    CAST(NULL AS NUMBER) AS rate
FROM {{ ref('maker__fact_vat_frob') }}
WHERE dart != 0

UNION ALL

SELECT 
    ilk,
    block_timestamp AS ts,
    tx_hash AS hash,
    dart/1e18,
    0 AS rate
FROM {{ ref('fact_vat_grab')}}
WHERE dart != 0

UNION ALL

SELECT 
    ilk,
    block_timestamp AS ts,
    tx_hash AS hash,
    CAST(NULL AS NUMBER) AS dart,
    rate
FROM {{ ref('maker__fact_vat_fold') }}
WHERE rate != 0