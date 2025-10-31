{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['gold','defi','stablecoins','heal','curated']
) }}

SELECT
    block_date,
    address,
    contract_address,
    symbol,
    NAME,
    decimals,
    balance,
    is_imputed,
    modified_timestamp,
    inserted_timestamp,
    stablecoins_supply_by_address_imputed_id AS ez_stablecoins_supply_id
FROM
    {{ ref('silver__stablecoins_supply_by_address_imputed') }}
    INNER JOIN {{ ref('defi__dim_stablecoins') }} USING (contract_address)
