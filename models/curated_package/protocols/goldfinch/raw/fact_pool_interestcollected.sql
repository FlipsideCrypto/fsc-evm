{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

SELECT 
    block_timestamp,
    tx_hash,
    contract_address,
    decoded_log:payer::string AS addr,
    decoded_log:poolAmount::number / 1e6 AS pool_amount,
    decoded_log:reserveAmount::number / 1e6 AS reserve_amount
FROM 
    {{ ref('core__ez_decoded_event_logs') }}
WHERE 
    event_name = 'InterestCollected'
    AND LOWER(contract_address) = '0xb01b315e32d1d9b5ce93e296d483e1f0aad39e75'