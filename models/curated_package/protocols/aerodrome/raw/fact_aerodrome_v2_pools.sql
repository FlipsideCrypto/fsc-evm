{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

SELECT 
    DECODED_LOG:pool::string as pool_address,
    DECODED_LOG:token0::string as token0_address,
    DECODED_LOG:token1::string as token1_address,
    DECODED_LOG:tickSpacing::integer as tick_spacing
FROM {{ ref('core__ez_decoded_event_logs') }} 
WHERE CONTRACT_ADDRESS = LOWER('0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A')
AND EVENT_NAME = 'PoolCreated'