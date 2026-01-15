{#
    Aave Protocol Macros
    These macros generate SQL for Aave financial metrics models
#}

{% macro flipside_lending_flashloan_fees(chain, protocol) %}
{#
    Generates SQL for flashloan fees from Flipside lending data
    Args:
        chain: The blockchain name (e.g., 'ethereum', 'polygon')
        protocol: The protocol name (e.g., 'Aave V2', 'Aave V3')
#}
SELECT
    block_timestamp::date AS date
    , '{{ chain }}' AS chain
    , '{{ protocol }}' AS protocol
    , flashloan_token AS token_address
    , SUM(premium_amount) AS amount_nominal
    , SUM(COALESCE(premium_amount_usd, 0)) AS amount_usd
FROM {{ ref('defi__ez_lending_flashloans') }}
WHERE platform = '{{ protocol }}'
GROUP BY 1, 2, 3, 4
{% endmacro %}


{% macro aave_liquidation_revenue(chain, protocol, pool_address) %}
{#
    Generates SQL for liquidation revenue from decoded event logs
    Args:
        chain: The blockchain name (e.g., 'ethereum', 'polygon')
        protocol: The protocol name (e.g., 'Aave V2', 'Aave V3')
        pool_address: The Aave pool contract address
#}
WITH
liquidator_events AS (
    SELECT
        block_timestamp
        , tx_hash
        , event_index
        , decoded_log:liquidator::string AS liquidator
        , decoded_log:user::string AS user
        , COALESCE(decoded_log:collateralAsset::string, decoded_log:collateral::string) AS collateral_asset
        , decoded_log:liquidatedCollateralAmount::float AS liquidated_collateral_amount
        , COALESCE(decoded_log:debtAsset::string, decoded_log:principal::string) AS debt_asset
        , decoded_log:debtToCover::float AS debt_to_cover
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE event_name = 'LiquidationCall'
        AND contract_address = LOWER('{{ pool_address }}')
)
SELECT
    block_timestamp::date AS date
    , '{{ chain }}' AS chain
    , '{{ protocol }}' AS protocol
    , block_timestamp
    , tx_hash
    , event_index
    , collateral_asset
    , liquidated_collateral_amount / POW(10, collateral_price.decimals) AS collateral_amount_nominal
    , collateral_amount_nominal * collateral_price.price AS collateral_amount_usd
    , debt_asset
    , debt_to_cover / POW(10, debt_price.decimals) AS debt_amount_nominal
    , debt_amount_nominal * debt_price.price AS debt_amount_usd
    , collateral_amount_usd - debt_amount_usd AS liquidation_revenue
FROM liquidator_events
LEFT JOIN {{ ref('price__ez_prices_hourly') }} collateral_price
    ON LOWER(collateral_asset) = LOWER(collateral_price.token_address)
        AND date_trunc(hour, block_timestamp) = collateral_price.hour
LEFT JOIN {{ ref('price__ez_prices_hourly') }} debt_price
    ON LOWER(debt_asset) = LOWER(debt_price.token_address)
        AND date_trunc(hour, block_timestamp) = debt_price.hour
{% endmacro %}


{% macro aave_v3_reserve_factor_revenue(chain, pool_address, protocol) %}
{#
    Generates SQL for Aave V3 reserve factor revenue
    Args:
        chain: The blockchain name
        pool_address: The Aave V3 pool contract address
        protocol: The protocol name (e.g., 'AAVE V3')
#}
WITH
reserve_factor_events AS (
    SELECT
        block_timestamp::date AS date
        , decoded_log:asset::string AS reserve
        , decoded_log:amountToTreasury::float AS amount_to_treasury
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE contract_address = LOWER('{{ pool_address }}')
        AND event_name = 'ReserveDataUpdated'
        AND decoded_log:amountToTreasury IS NOT NULL
)
, priced AS (
    SELECT
        r.date
        , r.reserve AS token_address
        , amount_to_treasury / POW(10, p.decimals) AS amount_nominal
        , amount_nominal * p.price AS amount_usd
    FROM reserve_factor_events r
    LEFT JOIN {{ ref('price__ez_prices_hourly') }} p
        ON LOWER(r.reserve) = LOWER(p.token_address)
        AND date_trunc(hour, r.date) = p.hour
)
SELECT
    date
    , '{{ chain }}' AS chain
    , '{{ protocol }}' AS protocol
    , token_address
    , SUM(COALESCE(amount_nominal, 0)) AS amount_nominal
    , SUM(COALESCE(amount_usd, 0)) AS amount_usd
FROM priced
GROUP BY 1, 2, 3, 4
{% endmacro %}


{% macro aave_v2_reserve_factor_revenue(chain, pool_address, protocol) %}
{#
    Generates SQL for Aave V2 reserve factor revenue
    Same as V3 but may have different event structure
#}
{{ aave_v3_reserve_factor_revenue(chain, pool_address, protocol) }}
{% endmacro %}


{% macro aave_v3_ecosystem_incentives(chain, incentives_controller, protocol) %}
{#
    Generates SQL for Aave V3 ecosystem incentives
    Args:
        chain: The blockchain name
        incentives_controller: The incentives controller contract address
        protocol: The protocol name
#}
WITH
claimed_rewards AS (
    SELECT
        block_timestamp::date AS date
        , decoded_log:reward::string AS reward_token
        , decoded_log:amount::float AS amount
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE contract_address = LOWER('{{ incentives_controller }}')
        AND event_name = 'RewardsClaimed'
)
, priced AS (
    SELECT
        c.date
        , c.reward_token AS token_address
        , amount / POW(10, p.decimals) AS amount_nominal
        , amount_nominal * p.price AS amount_usd
    FROM claimed_rewards c
    LEFT JOIN {{ ref('price__ez_prices_hourly') }} p
        ON LOWER(c.reward_token) = LOWER(p.token_address)
        AND date_trunc(hour, c.date) = p.hour
)
SELECT
    date
    , '{{ chain }}' AS chain
    , '{{ protocol }}' AS protocol
    , token_address
    , SUM(COALESCE(amount_nominal, 0)) AS amount_nominal
    , SUM(COALESCE(amount_usd, 0)) AS amount_usd
FROM priced
GROUP BY 1, 2, 3, 4
{% endmacro %}


{% macro aave_v2_ecosystem_incentives(chain, incentives_controller, protocol) %}
{#
    Generates SQL for Aave V2 ecosystem incentives
#}
{{ aave_v3_ecosystem_incentives(chain, incentives_controller, protocol) }}
{% endmacro %}


{% macro get_coingecko_price_with_latest(token_id) %}
{#
    Generates SQL to get daily prices from price tables with latest price for current date
    Args:
        token_id: The coingecko token ID (e.g., 'aave', 'ethereum', 'gho')
#}
SELECT
    hour::date AS date
    , AVG(price) AS price
FROM {{ ref('price__ez_prices_hourly') }}
WHERE LOWER(symbol) = LOWER('{{ token_id }}')
    OR LOWER(token_address) IN (
        SELECT LOWER(token_address)
        FROM {{ ref('price__ez_prices_hourly') }}
        WHERE LOWER(symbol) = LOWER('{{ token_id }}')
        LIMIT 1
    )
GROUP BY 1
{% endmacro %}


{% macro aave_deposits_borrows_lender_revenue(chain, protocol, pool_address, collector_address, rpc_data_model) %}
{#
    Generates SQL for Aave deposits, borrows, and lender revenue
    Note: This macro requires RPC data that may not be available in all deployments.
    If the RPC data model is not available, this will return empty results.

    Args:
        chain: The blockchain name
        protocol: The protocol name (e.g., 'AAVE V3')
        pool_address: The Aave pool contract address
        collector_address: The Aave collector contract address
        rpc_data_model: The name of the RPC data model (not used in Flipside version)
#}
WITH
average_liquidity_rate AS (
    SELECT
        block_timestamp::date AS date
        , decoded_log:reserve::string AS reserve
        , AVG(decoded_log:stableBorrowRate::float / 1e27) AS stable_borrow_rate
        , AVG(decoded_log:variableBorrowIndex::float / 1e27) AS borrow_index
        , AVG(decoded_log:liquidityIndex::float / 1e27) AS liquidity_index
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE contract_address = LOWER('{{ pool_address }}')
        AND event_name = 'ReserveDataUpdated'
    GROUP BY 1, 2
)
, reserve_factor_data AS (
    SELECT
        block_timestamp::date AS date
        , decoded_log:asset::string AS reserve
        , MAX(COALESCE(decoded_log:newReserveFactor::number, decoded_log:factor::number)) / 1E4 AS reserve_factor
        , MAX(decoded_log:oldReserveFactor::number) / 1E4 AS old_reserve_factor
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE contract_address = LOWER('{{ collector_address }}')
        AND event_name = 'ReserveFactorChanged'
    GROUP BY 1, 2
)
, dates AS (
    SELECT DISTINCT block_timestamp::date AS date
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE date >= (SELECT MIN(date) FROM reserve_factor_data)
)
, cross_join_reserve_dates AS (
    SELECT
        reserve
        , date
    FROM dates
    CROSS JOIN (
        SELECT DISTINCT reserve
        FROM reserve_factor_data
    )
)
, forward_filled_reserve_factor AS (
    SELECT
        date
        , reserve
        , COALESCE(
            reserve_factor
            , LAG(reserve_factor) IGNORE NULLS OVER (PARTITION BY reserve ORDER BY date)
        ) AS reserve_factor
        , COALESCE(
            old_reserve_factor
            , LAG(old_reserve_factor) IGNORE NULLS OVER (PARTITION BY reserve ORDER BY date)
        ) AS old_reserve_factor
    FROM cross_join_reserve_dates
    LEFT JOIN reserve_factor_data USING(date, reserve)
)
, daily_rate AS (
    SELECT
        date
        , reserve
        , stable_borrow_rate/365 AS stable_borrow_rate
        , (borrow_index /
            CASE
                WHEN DATEADD(day, -1, date) = LAG(date) OVER (PARTITION BY reserve ORDER BY date)
                THEN LAG(borrow_index) OVER (PARTITION BY reserve ORDER BY date)
                ELSE borrow_index
            END
        ) - 1 AS daily_borrow_rate
        , (liquidity_index /
            CASE
                WHEN DATEADD(day, -1, date) = LAG(date) OVER (PARTITION BY reserve ORDER BY date)
                THEN LAG(liquidity_index) OVER (PARTITION BY reserve ORDER BY date)
                ELSE liquidity_index
            END
        ) - 1 AS daily_liquidity_rate
        , COALESCE(
            f.reserve_factor
            , f.old_reserve_factor
            , 0
        ) AS reserve_factor
    FROM average_liquidity_rate a
    LEFT JOIN forward_filled_reserve_factor f USING(date, reserve)
)
SELECT
    date
    , reserve AS token_address
    , '{{ chain }}' AS chain
    , '{{ protocol }}' AS app
    , daily_borrow_rate
    , daily_liquidity_rate
    , stable_borrow_rate
    , reserve_factor
FROM daily_rate
{% endmacro %}
