{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'PERPS, TRADES' } } },
    tags = ['gold','defi','perps','curated','ez']
) }}

{#
GMX v2 Perpetual Trades - Gold/EZ Table
Joins position changes with fees and market metadata for a complete view of perp trades.
#}

WITH position_changes AS (
    SELECT
        pc.block_number,
        pc.block_timestamp,
        pc.tx_hash,
        pc.origin_function_signature,
        pc.origin_from_address,
        pc.origin_to_address,
        pc.contract_address,
        pc.event_index,
        pc.event_name,
        pc.trader,
        pc.market,
        pc.collateral_token,
        pc.trade_direction,
        pc.trade_type,
        pc.is_long,
        pc.size_usd,
        pc.volume_usd,
        pc.execution_price,
        pc.price_impact_usd,
        pc.order_type,
        pc.order_key,
        pc.position_key,
        pc.platform,
        pc.protocol,
        pc.version,
        pc._log_id,
        pc.modified_timestamp
    FROM {{ ref('silver_perps__gmx_v2_position_changes') }} pc
),

fees AS (
    SELECT
        order_key,
        collateral_symbol,
        position_fee_usd,
        borrowing_fee_usd,
        liquidation_fee_usd,
        trader_discount_usd,
        total_fees_net_usd
    FROM {{ ref('silver_perps__gmx_v2_position_fees') }}
),

markets AS (
    SELECT
        market_address,
        market_name,
        index_token_symbol
    FROM {{ ref('silver_perps__gmx_v2_markets') }}
)

SELECT
    pc.block_number,
    pc.block_timestamp,
    pc.tx_hash,
    pc.origin_function_signature,
    pc.origin_from_address,
    pc.origin_to_address,
    pc.contract_address,
    pc.event_index,

    -- Trader info
    pc.trader,

    -- Market info
    pc.market AS market_address,
    COALESCE(m.market_name, 'UNKNOWN/USD') AS market,
    COALESCE(m.index_token_symbol, 'UNKNOWN') AS index_token_symbol,

    -- Collateral info
    pc.collateral_token,
    COALESCE(f.collateral_symbol, 'UNKNOWN') AS collateral_symbol,

    -- Trade details
    pc.trade_type,
    pc.trade_direction,
    pc.is_long,
    pc.event_name,

    -- Sizes
    ROUND(pc.volume_usd, 2) AS volume_usd,
    ROUND(pc.size_usd, 2) AS position_size_usd,

    -- Prices
    pc.execution_price,
    ROUND(pc.price_impact_usd, 2) AS price_impact_usd,

    -- Fees
    ROUND(COALESCE(f.position_fee_usd, 0), 2) AS trading_fee_usd,
    ROUND(COALESCE(f.borrowing_fee_usd, 0), 2) AS borrowing_fee_usd,
    ROUND(COALESCE(f.liquidation_fee_usd, 0), 2) AS liquidation_fee_usd,
    ROUND(COALESCE(f.trader_discount_usd, 0), 2) AS fee_discount_usd,
    ROUND(COALESCE(f.total_fees_net_usd, 0), 2) AS total_fees_usd,

    -- Order info
    pc.order_type,
    pc.order_key,
    pc.position_key,

    -- Platform
    pc.platform,
    pc.protocol,
    pc.version AS protocol_version,

    -- IDs
    pc._log_id,
    {{ dbt_utils.generate_surrogate_key(['pc._log_id']) }} AS ez_perp_trades_id,
    pc.modified_timestamp

FROM position_changes pc
LEFT JOIN fees f
    ON pc.order_key = f.order_key
LEFT JOIN markets m
    ON pc.market = m.market_address
