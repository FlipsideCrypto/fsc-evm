{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEX, LIQUIDITY, POOLS, LP, SWAPS',
    } } },
    tags = ['gold','defi','dex','curated']
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    liquidity_provider,
    sender,
    receiver,
    pool_address,
    pool_name,
    tokens,
    symbols,
    decimals,
    amounts_unadj,
    amounts,
    amounts_usd,
    tokens_is_verified,
    platform,
    protocol,
    version AS protocol_version,
    complete_dex_liquidity_pool_actions_id AS ez_dex_liquidity_pool_actions_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver_dex__complete_dex_liquidity_pool_actions') }}
