{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['tx_hash', 'event_index'],
    cluster_by = ['block_timestamp::date'],
    tags = ['silver_protocols', 'balancer', 'v2', 'pool_balance_changed', 'curated']
) }}

{#
    Balancer V2 Pool Balance Changed - Consolidated Cross-Chain Model

    Tracks PoolBalanceChanged events from Balancer V2 Vault contract.
    Captures liquidity additions and removals.
    Works across all chains - uses GLOBAL_PROJECT_NAME for chain identification.

    Deployments by chain (canonical Vault address):
    - ethereum, polygon, arbitrum, gnosis: 0xba12222222228d8ba445958a75a0704d566bf2c8
#}

{# Get vault address mapping from centralized vars #}
{% set v2_vaults = vars.CURATED_BALANCER_V2_VAULT %}

{# Get current chain and check if Balancer V2 is deployed #}
{% set chain = vars.GLOBAL_PROJECT_NAME %}
{% set has_v2 = chain in v2_vaults %}

{% if has_v2 %}
{% set vault_address = v2_vaults[chain] %}

WITH pool_balance_events AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        decoded_log:poolId::string AS pool_id,
        decoded_log:liquidityProvider::string AS liquidity_provider,
        decoded_log:tokens AS tokens,
        decoded_log:deltas AS deltas,
        decoded_log:protocolFeeAmounts AS protocol_fee_amounts,
        modified_timestamp
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE contract_address = LOWER('{{ vault_address }}')
        AND event_name = 'PoolBalanceChanged'
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
    {% endif %}
)

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    '{{ vars.GLOBAL_PROJECT_NAME }}' AS chain,
    'BALANCER V2' AS protocol,
    pool_id,
    liquidity_provider,
    tokens,
    deltas,
    protocol_fee_amounts,
    modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM pool_balance_events

{% else %}
{# Chain does not have Balancer V2 - return empty result set #}
SELECT
    NULL::NUMBER AS block_number,
    NULL::TIMESTAMP AS block_timestamp,
    NULL::STRING AS tx_hash,
    NULL::NUMBER AS event_index,
    '{{ vars.GLOBAL_PROJECT_NAME }}' AS chain,
    NULL::STRING AS protocol,
    NULL::STRING AS pool_id,
    NULL::STRING AS liquidity_provider,
    NULL::VARIANT AS tokens,
    NULL::VARIANT AS deltas,
    NULL::VARIANT AS protocol_fee_amounts,
    NULL::TIMESTAMP AS modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
WHERE FALSE
{% endif %}
