{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['tx_hash', 'contract_address'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_protocols', 'goldfinch', 'seniorpool', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Goldfinch Senior Pool - Reserve Funds Collected Events

    Tracks ReserveFundsCollected events from the Senior Pool contract.
    Contract: 0x8481a6ebaf5c7dabc3f7e09e44a89531fd31f822
#}

SELECT
    block_timestamp,
    tx_hash,
    contract_address,
    NULL AS addr,
    decoded_log:amount::NUMBER / 1e6 AS amount,
    modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM {{ ref('core__ez_decoded_event_logs') }}
WHERE event_name = 'ReserveFundsCollected'
AND LOWER(contract_address) = '0x8481a6ebaf5c7dabc3f7e09e44a89531fd31f822'
{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
    FROM {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
{% endif %}
