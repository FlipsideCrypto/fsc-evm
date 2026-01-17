{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['tx_hash', 'contract_address'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_protocols', 'goldfinch', 'migratedtranchedpool', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Goldfinch Migrated Tranched Pool - Withdrawal Made Events

    Tracks WithdrawalMade events from migrated tranched pool contracts.
#}

SELECT
    block_timestamp,
    tx_hash,
    contract_address,
    decoded_log:owner::STRING AS addr,
    decoded_log:interestWithdrawn::NUMBER / 1e6 AS interest_withdrawn,
    decoded_log:principalWithdrawn::NUMBER / 1e6 AS principal_withdrawn,
    modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM {{ ref('core__ez_decoded_event_logs') }}
WHERE event_name = 'WithdrawalMade'
AND (
    contract_address IN (
        SELECT migratedtranchepool_address
        FROM {{ ref('silver_protocols__goldfinch_migratedtranchepools_addresses') }}
    )
    OR contract_address = LOWER('0xd43a4f3041069c6178b99d55295b00d0db955bb5')
)
{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
    FROM {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
{% endif %}
