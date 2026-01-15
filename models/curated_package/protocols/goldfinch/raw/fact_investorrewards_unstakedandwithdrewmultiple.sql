{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

SELECT 
    block_timestamp,
    tx_hash,
    contract_address,
    'withdraw' AS tx_type,
    decoded_log:user::string AS addr,
    decoded_log:usdcReceivedAmount::number / 1e6 AS amount
FROM 
    {{ ref('core__ez_decoded_event_logs') }}
WHERE 
    event_name = 'UnstakedAndWithdrewMultiple'
    AND LOWER(contract_address) = '0xfd6ff39da508d281c2d255e9bbbfab34b6be60c3'