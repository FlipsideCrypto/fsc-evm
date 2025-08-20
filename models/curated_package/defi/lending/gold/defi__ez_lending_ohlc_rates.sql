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
    token_symbol,
    -- Supply Rate OHLC
    supply_rate_open,
    supply_rate_high,
    supply_rate_low,
    supply_rate_close,
    -- Stable Borrow Rate OHLC
    stable_borrow_rate_open,
    stable_borrow_rate_high,
    stable_borrow_rate_low,
    stable_borrow_rate_close,
    -- Variable Borrow Rate OHLC
    variable_borrow_rate_open,
    variable_borrow_rate_high,
    variable_borrow_rate_low,
    variable_borrow_rate_close,
    rate_updates_count,
    blockchain,
    COALESCE (
        aave_interest_rates_ohlc_id,
        {{ dbt_utils.generate_surrogate_key(
            ['day', 'protocol', 'platform', 'version', 'token_address']
        ) }}
    ) AS ez_ohlc_rates_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM 
    {{ ref('silver_lending__aave_ohlc_interest_rates') }}
