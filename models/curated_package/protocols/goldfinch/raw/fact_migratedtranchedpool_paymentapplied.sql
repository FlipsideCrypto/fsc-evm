{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

SELECT 
    block_timestamp,
    tx_hash,
    contract_address,
    decoded_log:payer::string AS addr,
    decoded_log:interestAmount::number / 1e6 AS interest_amount,
    decoded_log:principalAmount::number / 1e6 AS principal_amount,
    decoded_log:reserveAmount::number / 1e6 AS reserve_amount
FROM 
    {{ ref('core__ez_decoded_event_logs') }}
WHERE 
    event_name = 'PaymentApplied'
    AND contract_address IN (SELECT migratedtranchepool_address FROM {{ref('dim_migratedtranchepools_addresses')}})