{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['tx_hash', 'contract_address'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_protocols', 'goldfinch', 'investorrewards', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Goldfinch Investor Rewards - Unstaked And Withdrew Events

    Tracks UnstakedAndWithdrew events from the StakingRewards contract.
    Contract: 0xfd6ff39da508d281c2d255e9bbbfab34b6be60c3
#}

SELECT
    block_timestamp,
    tx_hash,
    contract_address,
    'withdraw' AS tx_type,
    decoded_log:user::STRING AS addr,
    decoded_log:usdcReceivedAmount::NUMBER / 1e6 AS amount,
    modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM {{ ref('core__ez_decoded_event_logs') }}
WHERE event_name = 'UnstakedAndWithdrew'
AND LOWER(contract_address) = '0xfd6ff39da508d281c2d255e9bbbfab34b6be60c3'
{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
    FROM {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
{% endif %}
