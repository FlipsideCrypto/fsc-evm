{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['pool_id'],
    cluster_by = ['block_timestamp::date'],
    tags = ['silver_protocols', 'balancer', 'v2', 'pool_metadata', 'curated']
) }}

{#
    Balancer V2 Pool Metadata - Consolidated Cross-Chain Model

    Tracks PoolRegistered events from Balancer V2 Vault contract.
    Captures pool creation and registration metadata.
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

WITH pool_registered_events AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        decoded_log:poolId::string AS pool_id,
        decoded_log:poolAddress::string AS pool_address,
        decoded_log:specialization::number AS specialization,
        modified_timestamp
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE contract_address = LOWER('{{ vault_address }}')
        AND event_name = 'PoolRegistered'
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
    pool_address,
    CASE specialization
        WHEN 0 THEN 'GENERAL'
        WHEN 1 THEN 'MINIMAL_SWAP_INFO'
        WHEN 2 THEN 'TWO_TOKEN'
        ELSE 'UNKNOWN'
    END AS pool_specialization,
    modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM pool_registered_events

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
    NULL::STRING AS pool_address,
    NULL::STRING AS pool_specialization,
    NULL::TIMESTAMP AS modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
WHERE FALSE
{% endif %}
