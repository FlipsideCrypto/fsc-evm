{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'pool_id', 'token_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'balancer', 'v2', 'tvl', 'curated']
) }}

{#
    Balancer V2 TVL by Pool and Token - Consolidated Cross-Chain Model

    Tracks total value locked per pool per token based on PoolBalanceChanged events.
    Aggregates daily token deltas from liquidity events.
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
        block_timestamp::date AS date,
        decoded_log:poolId::string AS pool_id,
        decoded_log:tokens AS tokens,
        decoded_log:deltas AS deltas,
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
),

flattened_tokens AS (
    SELECT
        date,
        pool_id,
        tokens.value::string AS token_address,
        deltas.value::float AS delta,
        tokens.index AS token_index,
        modified_timestamp
    FROM pool_balance_events,
    LATERAL FLATTEN(input => tokens) tokens,
    LATERAL FLATTEN(input => deltas) deltas
    WHERE tokens.index = deltas.index
),

daily_deltas AS (
    SELECT
        date,
        pool_id,
        token_address,
        SUM(delta) AS daily_delta,
        MAX(modified_timestamp) AS modified_timestamp
    FROM flattened_tokens
    GROUP BY 1, 2, 3
)

SELECT
    date,
    '{{ vars.GLOBAL_PROJECT_NAME }}' AS chain,
    'BALANCER V2' AS protocol,
    pool_id,
    token_address,
    daily_delta AS token_delta,
    modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM daily_deltas

{% else %}
{# Chain does not have Balancer V2 - return empty result set #}
SELECT
    NULL::DATE AS date,
    '{{ vars.GLOBAL_PROJECT_NAME }}' AS chain,
    NULL::STRING AS protocol,
    NULL::STRING AS pool_id,
    NULL::STRING AS token_address,
    NULL::FLOAT AS token_delta,
    NULL::TIMESTAMP AS modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
WHERE FALSE
{% endif %}
