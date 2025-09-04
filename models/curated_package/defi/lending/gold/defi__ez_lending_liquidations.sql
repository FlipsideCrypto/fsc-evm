 {{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PURPOSE': 'LENDING, LIQUIDATIONS'
            }
        }
    },
    tags = ['gold','defi','lending','curated','ez']
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    event_name,
    event_index,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    platform,
    liquidator,
    borrower,
    protocol_market,
    collateral_token,
    collateral_token_symbol,
    liquidated_amount_unadj,
    liquidated_amount,
    liquidated_amount_usd,
    liquidated_amount_unadj as amount_unadj,
    liquidated_amount as amount,
    liquidated_amount_usd as amount_usd,
    debt_token,
    debt_token_symbol,
    repaid_amount_unadj,
    repaid_amount,
    repaid_amount_usd,
    complete_lending_liquidations_id AS ez_lending_liquidations_id,
    inserted_timestamp,
    modified_timestamp
FROM 
    {{ ref('silver_lending__complete_lending_liquidations') }}