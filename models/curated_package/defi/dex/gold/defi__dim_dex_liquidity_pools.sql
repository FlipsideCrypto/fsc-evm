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
    block_number AS creation_block,
    block_timestamp AS creation_time,
    tx_hash AS creation_tx,
    platform,
    protocol,
    version AS protocol_version,
    contract_address AS factory_address,
    pool_address,
    pool_name,
    tokens,
    symbols,
    decimals,
    complete_dex_liquidity_pools_id AS dim_dex_liquidity_pools_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver_dex__complete_dex_liquidity_pools') }}
