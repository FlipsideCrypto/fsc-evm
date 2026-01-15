{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

SELECT 
    block_timestamp,
    tx_hash,
    contract_address,
    decoded_log:operator::string AS addr,
    decoded_log:fiduCanceled::number / 1e18 AS fidu_canceled,
    decoded_log:reserveFidu::number / 1e18 AS reserve_fidu
FROM 
    {{ ref('core__ez_decoded_event_logs') }}
WHERE 
    event_name = 'WithdrawalCanceled'
    AND LOWER(contract_address) = '0x8481a6ebaf5c7dabc3f7e09e44a89531fd31f822'