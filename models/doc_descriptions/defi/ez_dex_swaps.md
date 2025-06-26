{% docs ez_dex_swaps_table_doc %}

## Table: ez_dex_swaps

This table provides a comprehensive view of token swap events across major decentralized exchanges (DEXs) on EVM blockchains. It standardizes swap data from different DEX protocols into a unified format, enabling cross-DEX analysis and DeFi trading insights.

### Key Features:
- **Multi-DEX Coverage**: Uniswap V2/V3, SushiSwap, Curve, Balancer, and more
- **USD Valuations**: Automatic price conversions for both sides of swaps
- **Data Quality Checks**: Filters out anomalous prices and wash trades
- **Standardized Format**: Consistent schema across different DEX implementations

### DEX Protocol Coverage:
| Platform | AMM Type | Key Features |
|----------|----------|--------------|
| Uniswap V2 | Constant Product | x*y=k, 0.3% fee |
| Uniswap V3 | Concentrated Liquidity | Custom fee tiers, price ranges |
| Curve | StableSwap | Optimized for stablecoins |
| Balancer | Weighted Pools | Multi-asset, custom weights |
| SushiSwap | Constant Product | Fork of Uniswap V2 |

### Data Quality Rules:
- **Price Divergence Check**: `amount_in_usd` and `amount_out_usd` must be within reasonable bounds
- **Nullification Logic**: USD amounts set to NULL when price slippage exceeds thresholds
- **Outlier Detection**: Removes likely wash trades and price manipulation

### Important Relationships:
- **Join with dim_dex_liquidity_pools**: Get pool metadata and token details
- **Join with fact_event_logs**: Access raw swap events
- **Join with ez_prices_hourly**: Verify token prices

### Sample Queries:

**Daily DEX Volume Analysis**
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
```

**Token Pair Trading Activity**
```sql
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
```

**Arbitrage Opportunity Detection**
```sql
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
```

**Whale Swap Detection**
```sql
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
```

**DEX Market Share Analysis**
```sql
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

### Critical Usage Notes:
- **USD Nullification**: Check for NULL USD values which indicate price anomalies
- **Platform Differences**: Each DEX may have unique fee structures and mechanics
- **Slippage Calculation**: Compare amount_in_usd vs amount_out_usd for trade efficiency
- **MEV Activity**: Large discrepancies might indicate sandwich attacks

{% enddocs %}

{% docs dim_dex_lp_table_doc %}

## Table: dim_dex_liquidity_pools

This dimensional table contains comprehensive metadata for all DEX liquidity pools across supported protocols. It provides essential information about pool composition, token pairs, and configuration needed for analyzing liquidity provision and pool performance.

### Key Features:
- **Multi-Protocol Support**: Covers all major AMM protocols
- **Token Metadata**: Symbols, decimals, and addresses for each pool token
- **Pool Configuration**: Fee tiers, pool types, and protocol-specific settings
- **Creation Tracking**: Block and timestamp of pool deployment

### Sample Queries:

**Pool Discovery by Token**
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
```

**New Pool Deployments**
```sql
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
```

### JSON Field Access:
```sql
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

**Calculation**: `amount_in_unadj / 10^token_decimals`
**Usage**: Human-readable token amounts for analysis

**Example**:
- Raw: 1000000 (6 decimals) = 1.0 USDC
- Raw: 1000000000000000000 (18 decimals) = 1.0 WETH

{% enddocs %}

{% docs ez_dex_swaps_amount_in_usd %}

USD value of tokens provided in the swap at time of transaction.

**Calculation**: `amount_in * token_price_usd`
**Quality Checks**: Set to NULL when:
- Price data unavailable
- Significant divergence from amount_out_usd (likely bad price)
- Suspected wash trade or manipulation

**Usage Note**: Always check for NULL before calculations

{% enddocs %}

{% docs ez_dex_swaps_amount_out %}

The decimal-adjusted quantity of tokens received by the trader from the swap.

**Note**: After DEX fees and slippage
**Slippage Calculation**: Compare with expected output based on pool reserves

{% enddocs %}

{% docs ez_dex_swaps_sender %}

The address that initiated the swap transaction.

**Common Patterns**:
- DEX Router contracts (aggregated swaps)
- EOA addresses (direct interaction)
- Smart contracts (automated strategies)

**Not Always End User**: May be intermediary contract

{% enddocs %}

{% docs ez_dex_swaps_to %}

The recipient address of the swapped tokens.

**Patterns**:
- Same as sender: User swapping for themselves
- Different address: Swapping on behalf of another
- Contract address: Part of larger transaction flow

**MEV Detection**: Check if different from sender for potential sandwich attacks

{% enddocs %}

{% docs ez_dex_swaps_platform %}

The DEX protocol where the swap occurred.

**Common Values**:
- uniswap_v2
- uniswap_v3
- sushiswap
- curve
- balancer_v2
- pancakeswap

**Usage**: Filter by platform for protocol-specific analysis

{% enddocs %}

{% docs ez_dex_swaps_pool_address %}

The liquidity pool contract address where the swap executed.

**Usage**:
- Join with dim_dex_liquidity_pools for pool metadata
- Track pool-specific volume and activity
- Analyze liquidity depth and utilization

{% enddocs %}

{% docs ez_dex_swaps_amount_in_unadj %}

The raw, non-decimal adjusted amount of tokens provided in the swap.

**Format**: Original blockchain value without decimal conversion
**Usage**: For precise calculations or verification against raw logs
**Relationship**: `amount_in = amount_in_unadj / 10^decimals`

{% enddocs %}

{% docs ez_dex_swaps_amount_out_unadj %}

The raw, non-decimal adjusted amount of tokens received from the swap.

**Format**: Original blockchain value without decimal conversion
**Usage**: For exact value matching with event logs
**Relationship**: `amount_out = amount_out_unadj / 10^decimals`

{% enddocs %}

{% docs ez_dex_swaps_amount_out_usd %}

USD value of tokens received from the swap at time of transaction.

**Calculation**: `amount_out * token_price_usd`
**Quality Check**: Compared with amount_in_usd for anomaly detection
**NULL When**: Price unavailable or significant divergence detected

{% enddocs %}

{% docs ez_dex_swaps_symbol_in %}

The ticker symbol of the token being sold/swapped from.

**Examples**: WETH, USDC, DAI, WBTC
**Source**: From token metadata or dim_contracts
**NULL**: For unverified or new tokens

{% enddocs %}

{% docs ez_dex_swaps_symbol_out %}

The ticker symbol of the token being bought/received.

**Examples**: USDC, WETH, UNI, AAVE
**Usage**: Human-readable pair identification
**Format**: Standard token symbols

{% enddocs %}

{% docs ez_dex_swaps_token_in %}

The contract address of the token being sold in the swap.

**Format**: Lowercase hex address (0x + 40 chars)
**Usage**: 
- Join with token metadata tables
- Precise token identification
- Track specific token flows

{% enddocs %}

{% docs ez_dex_swaps_token_out %}

The contract address of the token being received from the swap.

**Format**: Lowercase hex address (0x + 40 chars)
**Relationship**: Forms trading pair with token_in
**Usage**: Calculate price ratios and exchange rates

{% enddocs %}

{% docs ez_dex_swaps_creation_block %}

The block number when the liquidity pool was first created.

**Usage**:
- Calculate pool age
- Historical analysis starting point
- Join with fact_blocks for context

{% enddocs %}

{% docs ez_dex_swaps_creation_time %}

The timestamp when the liquidity pool was deployed.

**Format**: TIMESTAMP_NTZ
**Usage**:
- Pool age analysis
- Correlate with market events
- Filter new vs established pools

{% enddocs %}

{% docs ez_dex_swaps_creation_tx %}

The transaction hash that deployed this liquidity pool.

**Format**: 66-character hex string
**Usage**:
- Trace pool creation details
- Identify deployer and initial parameters
- Audit pool legitimacy

{% enddocs %}

{% docs ez_dex_swaps_factory_address %}

The factory contract that deployed this liquidity pool.

**Examples**:
- Uniswap V2 Factory: 0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f
- Uniswap V3 Factory: 0x1F98431c8aD98523631AE4a59f267346ea31F984

**Usage**: Identify pool protocol version and legitimacy

{% enddocs %}

{% docs ez_dex_swaps_pool_name %}

Human-readable name for the liquidity pool.

**Format Examples**:
- "WETH/USDC 0.05%" (Uniswap V3)
- "WETH-USDC" (Uniswap V2)
- Token addresses if symbols unavailable

**NULL**: For pools without readable token symbols

{% enddocs %}

{% docs ez_dex_swaps_decimals %}

JSON object containing decimal places for each token in the pool.

**Structure**:
```json
{
  "token0": 18,
  "token1": 6
}
```

**Access Pattern**:
```sql
SELECT 
    decimals:token0::integer AS token0_decimals,
    decimals:token1::integer AS token1_decimals
FROM <blockchain_name>.defi.dim_dex_liquidity_pools;
```

{% enddocs %}

{% docs ez_dex_swaps_symbols %}

JSON object containing token symbols for the pool pair.

**Structure**:
```json
{
  "token0": "WETH",
  "token1": "USDC"
}
```

**Query Example**:
```sql
SELECT 
    pool_address,
    symbols:token0::string || '/' || symbols:token1::string AS pair_name
FROM <blockchain_name>.defi.dim_dex_liquidity_pools
WHERE symbols:token0::string = 'WETH';
```

{% enddocs %}

{% docs ez_dex_swaps_tokens %}

JSON object containing token contract addresses in the pool.

**Structure**:
```json
{
  "token0": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
  "token1": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
}
```

**Usage**:
```sql
-- Find all pools with specific token
SELECT *
FROM <blockchain_name>.defi.dim_dex_liquidity_pools
WHERE tokens:token0::string = LOWER('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48')
   OR tokens:token1::string = LOWER('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48');
```

{% enddocs %}

{% docs ez_dex_swaps_token_in_is_verified %}

Whether the token in the swap is verified.

**Usage**:
- Filter for verified tokens
- Identify token quality and trustworthiness

{% enddocs %}

{% docs ez_dex_swaps_token_out_is_verified %}

Whether the token out of the swap is verified.

**Usage**:
- Filter for verified tokens
- Identify token quality and trustworthiness

{% enddocs %}

{% docs ez_dex_swaps_protocol_version %}

The version of the protocol used for the swap.

**Usage**:
- Identify protocol version
- Filter for specific protocol versions

{% enddocs %}

{% docs ez_dex_swaps_protocol %}

The protocol used for the swap. This is the clean name of the protocol, not the platform, without the version.

**Usage**:
- Identify protocol used
- Filter for specific protocols

{% enddocs %}

{% docs ez_dex_swaps_contract_address %}

The contract address of the swap. This is the address of the contract that executed the swap, often a pool contract.

**Usage**:
- Identify the contract address of the swap
- Filter for specific contracts

{% enddocs %}