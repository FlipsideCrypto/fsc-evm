{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['tx_hash', 'event_index'],
    cluster_by = ['block_timestamp::date'],
    tags = ['silver_protocols', 'balancer', 'v2', 'swaps', 'curated']
) }}

{#
    Balancer V2 Swaps - Consolidated Cross-Chain Model

    Tracks Swap events from Balancer V2 Vault contract.
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

WITH swap_events AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        decoded_log:poolId::string AS pool_id,
        decoded_log:tokenIn::string AS token_in,
        decoded_log:tokenOut::string AS token_out,
        decoded_log:amountIn::float AS amount_in,
        decoded_log:amountOut::float AS amount_out,
        modified_timestamp
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE contract_address = LOWER('{{ vault_address }}')
        AND event_name = 'Swap'
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
    token_in,
    token_out,
    amount_in,
    amount_out,
    modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM swap_events

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
    NULL::STRING AS token_in,
    NULL::STRING AS token_out,
    NULL::FLOAT AS amount_in,
    NULL::FLOAT AS amount_out,
    NULL::TIMESTAMP AS modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
WHERE FALSE
{% endif %}
