{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

SELECT DISTINCT t.tx_hash
FROM {{ ref('core__fact_traces') }} t
JOIN {{ ref('dim_maker_contracts') }} c
    ON t.from_address = c.contract_address
    AND c.contract_type IN ('FlapFlop')

UNION

SELECT '0x395e70dfbb3b3a23fbfd0e7a4ad659c77302e2f5923606e006e981097cc27ef9' as tx_hash