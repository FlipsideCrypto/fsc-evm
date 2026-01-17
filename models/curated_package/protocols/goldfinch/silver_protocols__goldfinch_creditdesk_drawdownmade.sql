{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['tx_hash', 'contract_address'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_protocols', 'goldfinch', 'creditdesk', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Goldfinch CreditDesk Drawdown Made Events

    Tracks DrawdownMade events from the CreditDesk contract.
    Contract: 0xd52dc1615c843c30f2e4668e101c0938e6007220
#}

SELECT
    block_timestamp,
    tx_hash,
    contract_address,
    'drawdown' AS tx_type,
    decoded_log:borrower::STRING AS addr,
    decoded_log:drawdownAmount::NUMBER / 1e6 AS amount,
    modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM {{ ref('core__ez_decoded_event_logs') }}
WHERE event_name = 'DrawdownMade'
AND LOWER(contract_address) = '0xd52dc1615c843c30f2e4668e101c0938e6007220'
{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
    FROM {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
{% endif %}
