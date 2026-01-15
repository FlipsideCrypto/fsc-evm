{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

SELECT 
    block_timestamp,
    tx_hash,
    contract_address,
    decoded_log:capitalProvider::string AS addr,
    decoded_log:amount::number / 1e6 AS amount
FROM 
    {{ ref('core__ez_decoded_event_logs') }}
WHERE 
    event_name = 'DepositMade'
    AND contract_address = lower('0x8481a6EbAf5c7DABc3F7e09e44A89531fd31F822')