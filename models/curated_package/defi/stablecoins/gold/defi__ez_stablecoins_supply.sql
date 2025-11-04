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
    blacklist_supply,
    bridge_balance,
    dex_balance,
    lending_pool_balance,
    contracts_balance,
    mint_amount,
    burn_amount,
    circulating_supply,
    transfer_volume,
    inserted_timestamp,
    modified_timestamp,
    stablecoins_supply_circulating_id AS ez_stablecoins_supply_id
FROM
    {{ ref('silver_stablecoins__supply_complete') }}
    INNER JOIN {{ ref('defi__dim_stablecoins') }} USING (contract_address)
