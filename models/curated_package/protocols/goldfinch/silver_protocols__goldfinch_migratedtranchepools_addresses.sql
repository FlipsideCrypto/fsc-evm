{{ config(
    materialized = 'table',
    tags = ['silver_protocols', 'goldfinch', 'dimension', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Goldfinch Migrated Tranche Pools Addresses

    This dimension table contains the contract addresses of migrated tranched pools
    identified by PaymentApplied events, excluding the CreditDesk contract.
#}

SELECT DISTINCT
    contract_address AS migratedtranchepool_address,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM {{ ref('core__ez_decoded_event_logs') }}
WHERE event_name = 'PaymentApplied'
AND contract_address <> '0xd52dc1615c843c30f2e4668e101c0938e6007220'
