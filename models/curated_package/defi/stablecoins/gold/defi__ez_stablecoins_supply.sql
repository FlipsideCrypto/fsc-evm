{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['gold','defi','stablecoins','heal','curated']
) }}

SELECT
    block_date,
    contract_address,
    symbol,
    NAME,
    decimals,
    total_supply,
    blacklist_supply,
    locked_in_bridges,
    mint_amount,
    burn_amount,
    circulating_supply,
    inserted_timestamp,
    modified_timestamp,
    stablecoins_supply_circulating_id AS ez_stablecoins_supply_id
FROM
    {{ ref('silver__stablecoins_supply_complete') }}
    INNER JOIN {{ ref('defi__dim_stablecoins') }} USING (contract_address)
