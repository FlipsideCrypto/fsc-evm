{# Get variables #}
{% set vars = return_vars() %}
{# Log configuration details #}
{{ log_model_details() }}
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
    amount_blacklisted,
    amount_in_bridges,
    amount_in_dexes,
    amount_in_lending_pools,
    amount_in_all_contracts,
    amount_minted,
    amount_burned,
    circulating_supply,
    amount_transferred,
    inserted_timestamp,
    modified_timestamp,
    stablecoins_supply_circulating_id AS ez_stablecoins_supply_id
FROM
    {{ ref('silver_stablecoins__supply_complete') }}
    INNER JOIN {{ ref('defi__dim_stablecoins') }} USING (contract_address)
