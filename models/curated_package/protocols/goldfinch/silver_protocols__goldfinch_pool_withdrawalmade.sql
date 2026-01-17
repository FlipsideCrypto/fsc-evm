{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['tx_hash', 'contract_address'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_protocols', 'goldfinch', 'pool', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Goldfinch Pool - Withdrawal Made Events

    Tracks WithdrawalMade events from the Pool contract.
    Contract: 0xB01b315e32D1D9B5CE93e296D483e1f0aAD39E75
#}

SELECT
    block_timestamp,
    tx_hash,
    contract_address,
    decoded_log:capitalProvider::STRING AS addr,
    decoded_log:userAmount::NUMBER / 1e6 AS user_amount,
    decoded_log:reserveAmount::NUMBER / 1e6 AS reserve_amount,
    modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM {{ ref('core__ez_decoded_event_logs') }}
WHERE event_name = 'WithdrawalMade'
AND contract_address = LOWER('0xB01b315e32D1D9B5CE93e296D483e1f0aAD39E75')
{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
    FROM {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
{% endif %}
