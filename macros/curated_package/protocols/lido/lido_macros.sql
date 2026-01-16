{#
    Lido Protocol Macros

    This file contains macros for Lido protocol models including:
    - get_treasury_balance: Track token balances in treasury addresses
    - lido_fee_split: Historical fee split percentages for Lido
#}

{% macro get_treasury_balance(chain, addresses, earliest_date, is_incremental_run=false, lookback_hours='48', lookback_days='7') %}
{#
    Tracks token balances flowing in/out of treasury address(es) over time.

    Args:
        chain: Chain identifier (e.g., 'ethereum')
        addresses: Single address string or list of addresses
        earliest_date: Start date for tracking (e.g., '2020-12-17')
        is_incremental_run: Boolean for incremental filtering
        lookback_hours: Hours to look back for modified_timestamp
        lookback_days: Days to look back as safety net

    Returns:
        Daily token balances by treasury address with USD values
#}

{# Handle both single address string and list of addresses #}
{% set address_list = addresses if addresses is iterable and addresses is not string else [addresses] %}

WITH
date_spine AS (
    SELECT DATEADD('day', seq4(), '{{ earliest_date }}'::date) AS date
    FROM TABLE(GENERATOR(ROWCOUNT => 10000))
    WHERE date <= CURRENT_DATE()
),
transfers AS (
    SELECT
        block_number
        , block_timestamp::date AS date
        , contract_address AS token_address
        , CASE
            WHEN LOWER(to_address) IN ({% for addr in address_list %}LOWER('{{ addr }}'){% if not loop.last %}, {% endif %}{% endfor %}) THEN amount
            ELSE 0
        END AS amount_in
        , CASE
            WHEN LOWER(from_address) IN ({% for addr in address_list %}LOWER('{{ addr }}'){% if not loop.last %}, {% endif %}{% endfor %}) THEN amount
            ELSE 0
        END AS amount_out
        , modified_timestamp
    FROM {{ ref('core__ez_token_transfers') }}
    WHERE (
        LOWER(to_address) IN ({% for addr in address_list %}LOWER('{{ addr }}'){% if not loop.last %}, {% endif %}{% endfor %})
        OR LOWER(from_address) IN ({% for addr in address_list %}LOWER('{{ addr }}'){% if not loop.last %}, {% endif %}{% endfor %})
    )
    {% if is_incremental_run %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ lookback_hours }} hours'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ lookback_days }} days'
    {% endif %}
),
daily_flows AS (
    SELECT
        date
        , token_address
        , SUM(amount_in) AS daily_in
        , SUM(amount_out) AS daily_out
        , SUM(amount_in) - SUM(amount_out) AS daily_net
        , MAX(block_number) AS block_number
        , MAX(modified_timestamp) AS modified_timestamp
    FROM transfers
    GROUP BY 1, 2
),
tokens AS (
    SELECT DISTINCT token_address FROM daily_flows
),
token_dates AS (
    SELECT
        d.date
        , t.token_address
    FROM date_spine d
    CROSS JOIN tokens t
),
daily_balances AS (
    SELECT
        td.date
        , td.token_address
        , COALESCE(df.daily_in, 0) AS daily_in
        , COALESCE(df.daily_out, 0) AS daily_out
        , COALESCE(df.daily_net, 0) AS daily_net
        , SUM(COALESCE(df.daily_net, 0)) OVER (PARTITION BY td.token_address ORDER BY td.date) AS balance
        , df.block_number
        , df.modified_timestamp
    FROM token_dates td
    LEFT JOIN daily_flows df
        ON td.date = df.date
        AND td.token_address = df.token_address
),
prices AS (
    SELECT
        hour::date AS date
        , token_address
        , AVG(price) AS price
        , MAX(decimals) AS decimals
        , MAX(symbol) AS symbol
    FROM {{ ref('price__ez_prices_hourly') }}
    GROUP BY 1, 2
)
SELECT
    db.date
    , '{{ chain }}' AS chain
    , db.token_address
    , p.symbol
    , db.balance AS balance_raw
    , COALESCE(db.balance / POW(10, p.decimals), db.balance) AS balance_nominal
    , COALESCE(balance_nominal * p.price, 0) AS balance_usd
    , db.daily_in AS daily_in_raw
    , db.daily_out AS daily_out_raw
    , db.block_number
    , COALESCE(db.modified_timestamp, SYSDATE()) AS modified_timestamp
FROM daily_balances db
LEFT JOIN prices p
    ON db.date = p.date
    AND LOWER(db.token_address) = LOWER(p.token_address)
WHERE db.balance != 0 OR db.daily_in != 0 OR db.daily_out != 0
{% endmacro %}


{% macro lido_fee_split() %}
{#
    Returns historical Lido fee split percentages.

    Lido takes a 10% fee on staking rewards, split between:
    - Treasury: 5% (insurance fund)
    - Node Operators: 5%

    Note: Fee structure changed over time - this provides the historical values.
#}

SELECT date, treasury_fee_pct, insurance_fee_pct, operators_fee_pct
FROM (
    VALUES
    -- Initial fee structure (Dec 2020 - present)
    -- 10% total fee: 5% to treasury/insurance, 5% to node operators
    ('2020-12-17'::date, 0.05, 0.00, 0.05)
) AS fee_history(date, treasury_fee_pct, insurance_fee_pct, operators_fee_pct)
{% endmacro %}
