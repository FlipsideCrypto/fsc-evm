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
    Goldfinch Pool - Principal Collected Events

    Tracks PrincipalCollected events from the Pool contract.
    Contract: 0xb01b315e32d1d9b5ce93e296d483e1f0aad39e75
#}

SELECT
    block_timestamp,
    tx_hash,
    contract_address,
    decoded_log:payer::STRING AS addr,
    decoded_log:amount::NUMBER / 1e6 AS amount,
    modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM {{ ref('core__ez_decoded_event_logs') }}
WHERE event_name = 'PrincipalCollected'
AND LOWER(contract_address) = '0xb01b315e32d1d9b5ce93e296d483e1f0aad39e75'
{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
    FROM {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
{% endif %}
