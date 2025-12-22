{{ config(
    materialized = 'view',
    persist_docs = {
        "relation": true,
        "columns": true
    },
    meta = {
        'database_tags': {
            'table': {
                'PURPOSE': 'ERC4626, VAULTS, DEPOSITS'
            }
        }
    },
    tags = ['gold','defi','erc4626','curated','ez']
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
    protocol,
    protocol_market,
    protocol_market_symbol,
    depositor,
    token_address,
    token_symbol,
    amount_unadj,
    amount,
    shares_unadj,
    shares,
    amount_usd,
    complete_erc4626_deposits_id AS ez_erc4626_deposits_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver_erc4626__complete_deposits') }}
