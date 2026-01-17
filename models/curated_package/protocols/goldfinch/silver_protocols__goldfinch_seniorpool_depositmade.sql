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
    Goldfinch Senior Pool - Deposit Made Events

    Tracks DepositMade events from the Senior Pool contract.
    Contract: 0x8481a6EbAf5c7DABc3F7e09e44A89531fd31F822
#}

SELECT
    block_timestamp,
    tx_hash,
    contract_address,
    decoded_log:capitalProvider::STRING AS addr,
    decoded_log:amount::NUMBER / 1e6 AS amount,
    modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM {{ ref('core__ez_decoded_event_logs') }}
WHERE event_name = 'DepositMade'
AND contract_address = LOWER('0x8481a6EbAf5c7DABc3F7e09e44A89531fd31F822')
{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
    FROM {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
{% endif %}
