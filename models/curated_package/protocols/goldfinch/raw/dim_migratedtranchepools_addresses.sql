{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

select distinct contract_address as migratedtranchepool_address from {{ ref('core__ez_decoded_event_logs') }}
where event_name = 'PaymentApplied'
and contract_address <> '0xd52dc1615c843c30f2e4668e101c0938e6007220'