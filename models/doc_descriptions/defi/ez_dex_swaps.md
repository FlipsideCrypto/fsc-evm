{% docs ez_dex_swaps_table_doc %}

## What

This table provides a comprehensive view of token swap events across major decentralized exchanges (DEXs) on EVM blockchains. It standardizes swap data from different DEX protocols into a unified format, enabling cross-DEX analysis and DeFi trading insights.

## Key Use Cases

- Analyzing DEX trading volumes and market share
- Tracking token pair liquidity and trading activity
- Detecting arbitrage opportunities across protocols
- Monitoring whale trades and unusual swap patterns
- Calculating slippage and price impact of trades

## Important Relationships

- **Join with dim_dex_liquidity_pools**: Get pool metadata and token details
- **Join with fact_event_logs**: Access raw swap events
- **Join with ez_prices_hourly**: Verify token prices

## Commonly-used Fields

- `platform`: DEX protocol (uniswap_v2, curve, etc.)
- `sender`: Address initiating the swap
- `token_in`/`token_out`: Token addresses being swapped
- `amount_in`/`amount_out`: Decimal-adjusted swap amounts
- `amount_in_usd`/`amount_out_usd`: USD values at swap time
- `pool_address`: Liquidity pool where swap occurred

## Sample queries

```sql
-- Daily swap volume by DEX platform
SELECT 
    DATE_TRUNC('day', block_timestamp) AS date,
    platform,
    COUNT(*) AS swap_count,
    COUNT(DISTINCT sender) AS unique_traders,
    COUNT(DISTINCT pool_address) AS active_pools,
    SUM(amount_in_usd) AS total_volume_usd,
    AVG(amount_in_usd) AS avg_swap_size_usd,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount_in_usd) AS median_swap_usd
FROM <blockchain_name>.defi.ez_dex_swaps
WHERE block_timestamp >= CURRENT_DATE - 30
    AND amount_in_usd IS NOT NULL
    AND amount_in_usd > 0
GROUP BY 1, 2
ORDER BY 1 DESC, 6 DESC;

-- Most active trading pairs
WITH pair_volume AS (
    SELECT 
        LEAST(token_in, token_out) AS token_a,
        GREATEST(token_in, token_out) AS token_b,
        LEAST(symbol_in, symbol_out) AS symbol_a,
        GREATEST(symbol_in, symbol_out) AS symbol_b,
        COUNT(*) AS swap_count,
        SUM(amount_in_usd) AS volume_usd,
        COUNT(DISTINCT sender) AS unique_traders,
        COUNT(DISTINCT DATE(block_timestamp)) AS active_days
    FROM <blockchain_name>.defi.ez_dex_swaps
    WHERE block_timestamp >= CURRENT_DATE - 7
        AND amount_in_usd IS NOT NULL
    GROUP BY 1, 2, 3, 4
)
SELECT 
    symbol_a || '/' || symbol_b AS pair,
    swap_count,
    volume_usd,
    unique_traders,
    active_days,
    volume_usd / swap_count AS avg_swap_size
FROM pair_volume
WHERE volume_usd > 100000
ORDER BY volume_usd DESC
LIMIT 50;

-- Price discrepancies across DEXs for same token pairs
WITH recent_swaps AS (
    SELECT 
        block_timestamp,
        platform,
        token_in,
        token_out,
        symbol_in,
        symbol_out,
        amount_in,
        amount_out,
        amount_in_usd / NULLIF(amount_in, 0) AS price_in_usd,
        amount_out_usd / NULLIF(amount_out, 0) AS price_out_usd,
        -- Calculate implied exchange rate
        amount_out / NULLIF(amount_in, 0) AS exchange_rate
    FROM <blockchain_name>.defi.ez_dex_swaps
    WHERE block_timestamp >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
        AND amount_in > 0 
        AND amount_out > 0
        AND amount_in_usd IS NOT NULL
),
price_comparison AS (
    SELECT 
        DATE_TRUNC('minute', block_timestamp) AS minute,
        token_in,
        token_out,
        symbol_in || '->' || symbol_out AS pair,
        platform,
        AVG(exchange_rate) AS avg_rate,
        COUNT(*) AS swap_count
    FROM recent_swaps
    GROUP BY 1, 2, 3, 4, 5
)
SELECT 
    p1.minute,
    p1.pair,
    p1.platform AS platform_1,
    p2.platform AS platform_2,
    p1.avg_rate AS rate_1,
    p2.avg_rate AS rate_2,
    ABS(p1.avg_rate - p2.avg_rate) / LEAST(p1.avg_rate, p2.avg_rate) * 100 AS price_diff_pct
FROM price_comparison p1
JOIN price_comparison p2
    ON p1.minute = p2.minute
    AND p1.token_in = p2.token_in
    AND p1.token_out = p2.token_out
    AND p1.platform < p2.platform
WHERE price_diff_pct > 1  -- More than 1% difference
ORDER BY p1.minute DESC, price_diff_pct DESC;

-- Large swaps by size and impact
SELECT 
    block_timestamp,
    tx_hash,
    platform,
    sender,
    symbol_in || ' -> ' || symbol_out AS swap_pair,
    amount_in,
    amount_in_usd,
    amount_out,
    amount_out_usd,
    ABS(amount_in_usd - amount_out_usd) / NULLIF(amount_in_usd, 0) * 100 AS slippage_pct
FROM <blockchain_name>.defi.ez_dex_swaps
WHERE block_timestamp >= CURRENT_DATE - 1
    AND amount_in_usd > 100000  -- Swaps over $100k
ORDER BY amount_in_usd DESC
LIMIT 100;

-- Platform market share by volume
WITH platform_stats AS (
    SELECT 
        platform,
        SUM(amount_in_usd) AS total_volume,
        COUNT(*) AS total_swaps,
        COUNT(DISTINCT sender) AS unique_users,
        COUNT(DISTINCT pool_address) AS unique_pools
    FROM <blockchain_name>.defi.ez_dex_swaps
    WHERE block_timestamp >= CURRENT_DATE - 7
        AND amount_in_usd IS NOT NULL
    GROUP BY 1
)
SELECT 
    platform,
    total_volume,
    ROUND(100.0 * total_volume / SUM(total_volume) OVER (), 2) AS market_share_pct,
    total_swaps,
    unique_users,
    unique_pools,
    total_volume / NULLIF(total_swaps, 0) AS avg_swap_size
FROM platform_stats
ORDER BY total_volume DESC;
```

{% enddocs %}

{% docs dim_dex_lp_table_doc %}

## What

This dimensional table contains comprehensive metadata for all DEX liquidity pools across supported protocols. It provides essential information about pool composition, token pairs, and configuration needed for analyzing liquidity provision and pool performance.

## Key Use Cases

- Finding all pools containing specific tokens
- Tracking new pool deployments
- Analyzing pool configurations and fee structures
- Identifying trading pairs across different protocols
- Monitoring factory contract deployments

## Important Relationships

- **Join with ez_dex_swaps**: Use `pool_address` to get swap activity
- **Join with dim_contracts**: Use token addresses for additional metadata
- **Self-join**: Find all pools with common tokens

## Commonly-used Fields

- `pool_address`: Unique identifier for the liquidity pool
- `platform`: DEX protocol (uniswap_v3, curve, etc.)
- `pool_name`: Human-readable pool identifier
- `tokens`: JSON with token0 and token1 addresses
- `symbols`: JSON with token0 and token1 symbols
- `creation_time`: When pool was deployed

## Sample queries

```sql
-- Find all pools containing USDC
SELECT 
    pool_address,
    pool_name,
    platform,
    creation_time,
    CASE 
        WHEN tokens:token0::string = LOWER('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48') THEN 
            symbols:token1::string
        ELSE 
            symbols:token0::string
    END AS paired_token
FROM <blockchain_name>.defi.dim_dex_liquidity_pools
WHERE LOWER('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48') IN (
    tokens:token0::string,
    tokens:token1::string
)
ORDER BY creation_time DESC;

-- Recently created liquidity pools
SELECT 
    platform,
    pool_address,
    pool_name,
    creation_time,
    creation_tx,
    symbols:token0::string || '/' || symbols:token1::string AS pair,
    factory_address
FROM <blockchain_name>.defi.dim_dex_liquidity_pools
WHERE creation_time >= CURRENT_DATE - 7
ORDER BY creation_time DESC
LIMIT 100;

-- Extract token information from JSON fields
SELECT 
    pool_address,
    tokens:token0::string AS token0_address,
    tokens:token1::string AS token1_address,
    symbols:token0::string AS token0_symbol,
    symbols:token1::string AS token1_symbol,
    decimals:token0::integer AS token0_decimals,
    decimals:token1::integer AS token1_decimals
FROM <blockchain_name>.defi.dim_dex_liquidity_pools
WHERE platform = 'uniswap_v3';
```

{% enddocs %}

{% docs ez_dex_swaps_amount_in %}

The decimal-adjusted quantity of tokens provided by the trader in the swap.

Example: 1000.5

{% enddocs %}

{% docs ez_dex_swaps_amount_in_usd %}

USD value of tokens provided in the swap at time of transaction.

Example: 1500.75

{% enddocs %}

{% docs ez_dex_swaps_amount_out %}

The decimal-adjusted quantity of tokens received by the trader from the swap.

Example: 0.65

{% enddocs %}

{% docs ez_dex_swaps_sender %}

The address that initiated the swap transaction.

Example: '0x7a250d5630b4cf539739df2c5dacb4c659f2488d'

{% enddocs %}

{% docs ez_dex_swaps_to %}

The recipient address of the swapped tokens.

Example: '0x1234567890123456789012345678901234567890'

{% enddocs %}

{% docs ez_dex_swaps_platform %}

The DEX protocol where the swap occurred.

Example: 'uniswap_v3'

{% enddocs %}

{% docs ez_dex_swaps_pool_address %}

The liquidity pool contract address where the swap executed.

Example: '0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8'

{% enddocs %}

{% docs ez_dex_swaps_amount_in_unadj %}

The raw, non-decimal adjusted amount of tokens provided in the swap.

Example: 1000500000

{% enddocs %}

{% docs ez_dex_swaps_amount_out_unadj %}

The raw, non-decimal adjusted amount of tokens received from the swap.

Example: 650000000000000000

{% enddocs %}

{% docs ez_dex_swaps_amount_out_usd %}

USD value of tokens received from the swap at time of transaction.

Example: 1498.25

{% enddocs %}

{% docs ez_dex_swaps_symbol_in %}

The ticker symbol of the token being sold/swapped from.

Example: 'USDC'

{% enddocs %}

{% docs ez_dex_swaps_symbol_out %}

The ticker symbol of the token being bought/received.

Example: 'WETH'

{% enddocs %}

{% docs ez_dex_swaps_token_in %}

The contract address of the token being sold in the swap.

Example: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'

{% enddocs %}

{% docs ez_dex_swaps_token_out %}

The contract address of the token being received from the swap.

Example: '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'

{% enddocs %}

{% docs ez_dex_swaps_creation_block %}

The block number when the liquidity pool was first created.

Example: 12369739

{% enddocs %}

{% docs ez_dex_swaps_creation_time %}

The timestamp when the liquidity pool was deployed.

Example: '2021-05-05 12:34:56.000'

{% enddocs %}

{% docs ez_dex_swaps_creation_tx %}

The transaction hash that deployed this liquidity pool.

Example: '0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef'

{% enddocs %}

{% docs ez_dex_swaps_factory_address %}

The factory contract that deployed this liquidity pool.

Example: '0x1f98431c8ad98523631ae4a59f267346ea31f984'

{% enddocs %}

{% docs ez_dex_swaps_pool_name %}

Human-readable name for the liquidity pool.

Example: 'WETH/USDC 0.05%'

{% enddocs %}

{% docs ez_dex_swaps_decimals %}

JSON object containing decimal places for each token in the pool.

Example: {"token0": 18, "token1": 6}

{% enddocs %}

{% docs ez_dex_swaps_symbols %}

JSON object containing token symbols for the pool pair.

Example: {"token0": "WETH", "token1": "USDC"}

{% enddocs %}

{% docs ez_dex_swaps_tokens %}

JSON object containing token contract addresses in the pool.

Example: {"token0": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", "token1": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"}

{% enddocs %}

{% docs ez_dex_swaps_token_in_is_verified %}

Whether the token in the swap is verified.

Example: true

{% enddocs %}

{% docs ez_dex_swaps_token_out_is_verified %}

Whether the token out of the swap is verified.

Example: true

{% enddocs %}

{% docs ez_dex_swaps_protocol_version %}

The version of the protocol used for the swap.

Example: 'v3'

{% enddocs %}

{% docs ez_dex_swaps_protocol %}

The protocol used for the swap. This is the clean name of the protocol, not the platform, without the version.

Example: 'uniswap'

{% enddocs %}

{% docs ez_dex_swaps_contract_address %}

The contract address of the swap. This is the address of the contract that executed the swap, often a pool contract.

Example: '0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8'

{% enddocs %}