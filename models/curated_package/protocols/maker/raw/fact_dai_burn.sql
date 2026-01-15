{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

SELECT
    block_timestamp,
    tx_hash,
    from_address as usr
FROM {{ ref('core__ez_token_transfers') }}
where to_address = '0x0000000000000000000000000000000000000000'
and lower(contract_address) = lower('0x6B175474E89094C44Da98b954EedeAC495271d0F')