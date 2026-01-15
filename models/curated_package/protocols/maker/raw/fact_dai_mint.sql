{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

SELECT
    block_timestamp,
    tx_hash,
    to_address as usr,
    raw_amount_precise as wad
FROM {{ ref('core__ez_token_transfers') }}
where from_address = '0x0000000000000000000000000000000000000000'
and lower(contract_address) = lower('0x6B175474E89094C44Da98b954EedeAC495271d0F')