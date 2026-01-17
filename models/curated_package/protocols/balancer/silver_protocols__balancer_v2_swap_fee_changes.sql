{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['tx_hash', 'event_index'],
    cluster_by = ['block_timestamp::date'],
    tags = ['silver_protocols', 'balancer', 'v2', 'swap_fee_changes', 'curated']
) }}

{#
    Balancer V2 Swap Fee Changes - Consolidated Cross-Chain Model

    Tracks SwapFeePercentageChanged events from Balancer V2 pool contracts.
    Note: These events are emitted by individual pool contracts, not the Vault.
    We filter to pools registered with the Balancer V2 Vault.
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

WITH registered_pools AS (
    SELECT DISTINCT
        decoded_log:poolAddress::string AS pool_address
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE contract_address = LOWER('{{ vault_address }}')
        AND event_name = 'PoolRegistered'
),

swap_fee_events AS (
    SELECT
        e.block_number,
        e.block_timestamp,
        e.tx_hash,
        e.event_index,
        e.contract_address AS pool_address,
        e.decoded_log:swapFeePercentage::float / 1e18 AS swap_fee_percentage,
        e.modified_timestamp
    FROM {{ ref('core__ez_decoded_event_logs') }} e
    INNER JOIN registered_pools p
        ON LOWER(e.contract_address) = LOWER(p.pool_address)
    WHERE e.event_name = 'SwapFeePercentageChanged'
    {% if is_incremental() %}
    AND e.modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
        FROM {{ this }}
    )
    AND e.modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
    {% endif %}
)

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    '{{ vars.GLOBAL_PROJECT_NAME }}' AS chain,
    'BALANCER V2' AS protocol,
    pool_address,
    swap_fee_percentage,
    modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM swap_fee_events

{% else %}
{# Chain does not have Balancer V2 - return empty result set #}
SELECT
    NULL::NUMBER AS block_number,
    NULL::TIMESTAMP AS block_timestamp,
    NULL::STRING AS tx_hash,
    NULL::NUMBER AS event_index,
    '{{ vars.GLOBAL_PROJECT_NAME }}' AS chain,
    NULL::STRING AS protocol,
    NULL::STRING AS pool_address,
    NULL::FLOAT AS swap_fee_percentage,
    NULL::TIMESTAMP AS modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
WHERE FALSE
{% endif %}
