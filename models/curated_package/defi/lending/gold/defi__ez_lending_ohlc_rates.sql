{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PURPOSE': 'LENDING, OHLC INTEREST RATES'
            }
        }
    },
    tags = ['gold','defi','lending','curated','ez','ohlc','interest_rates']
) }}

SELECT
    day,
    protocol,
    platform,
    version,
    token_address,
    supply_rate_open,
    supply_rate_high,
    supply_rate_low,
    supply_rate_close,
    stable_borrow_rate_open,
    stable_borrow_rate_high,
    stable_borrow_rate_low,
    stable_borrow_rate_close,
    variable_borrow_rate_open,
    variable_borrow_rate_high,
    variable_borrow_rate_low,
    variable_borrow_rate_close,
    rate_updates_count,
    blockchain,
    aave_interest_rates_ohlc_id AS ez_ohlc_rates_id,
    inserted_timestamp,
    modified_timestamp
FROM 
    {{ ref('silver_lending__aave_ohlc_interest_rates') }}
