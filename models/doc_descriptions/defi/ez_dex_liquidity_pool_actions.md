{% docs ez_dex_liquidity_pool_actions_table_doc %}

## What

This table provides a comprehensive view of liquidity pool actions across major decentralized exchanges (DEXs) on EVM blockchains. It standardizes liquidity provision and removal events from different DEX protocols into a unified format, enabling cross-DEX analysis of liquidity provider behavior and pool dynamics.

## Key Use Cases

- Analyzing liquidity provision patterns and LP behavior
- Tracking pool liquidity changes over time
- Monitoring large liquidity additions/removals (whale LP activity)
- Calculating LP rewards and impermanent loss
- Identifying new liquidity pools and their initial providers
- Analyzing protocol-specific LP incentives and migration patterns

## Important Relationships

- **Join with dim_dex_liquidity_pools**: Get pool metadata and token details
- **Join with ez_dex_swaps**: Correlate LP actions with trading activity
- **Join with fact_event_logs**: Access raw LP events
- **Join with ez_prices_hourly**: Calculate USD values at action time

## Commonly-used Fields

- `platform`: DEX protocol (uniswap-v3, uniswap-v2 etc.)
- `event_name`: Type of action (Mint, Burn, AddLiquidity, RemoveLiquidity etc.)
- `liquidity_provider`: Address providing/removing liquidity
- `pool_address`: Liquidity pool where action occurred
- `amounts`: JSON object with decimal-adjusted token amounts
- `amounts_usd`: JSON object with USD values at action time

## Sample queries

```sql
-- Daily liquidity provision by DEX platform
SELECT 
    DATE_TRUNC('day', block_timestamp) AS date,
    platform,
    event_name,
    COUNT(*) AS action_count,
    COUNT(DISTINCT liquidity_provider) AS unique_lps,
    COUNT(DISTINCT pool_address) AS active_pools
FROM <blockchain_name>.defi.ez_dex_liquidity_pool_actions
WHERE block_timestamp >= CURRENT_DATE - 30
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC;

-- Largest liquidity additions by USD value
SELECT 
    block_timestamp,
    tx_hash,
    platform,
    pool_name,
    liquidity_provider,
    event_name,
    CASE 
        WHEN symbols:token2::string IS NOT NULL THEN 
            symbols:token0::string || '/' || symbols:token1::string || '/' || symbols:token2::string || '...'
        ELSE 
            symbols:token0::string || '/' || symbols:token1::string 
    END AS pair,
    COALESCE(amounts_usd:token0::float, 0) + 
    COALESCE(amounts_usd:token1::float, 0) + 
    COALESCE(amounts_usd:token2::float, 0) + 
    COALESCE(amounts_usd:token3::float, 0) + 
    COALESCE(amounts_usd:token4::float, 0) + 
    COALESCE(amounts_usd:token5::float, 0) + 
    COALESCE(amounts_usd:token6::float, 0) + 
    COALESCE(amounts_usd:token7::float, 0) AS total_usd_value
FROM <blockchain_name>.defi.ez_dex_liquidity_pool_actions
WHERE block_timestamp >= CURRENT_DATE - 7
    AND event_name IN ('Mint', 'AddLiquidity')
    AND (amounts_usd:token0::float IS NOT NULL OR amounts_usd:token1::float IS NOT NULL)
ORDER BY total_usd_value DESC
LIMIT 50;

-- LP concentration analysis
WITH lp_stats AS (
    SELECT 
        liquidity_provider,
        COUNT(DISTINCT pool_address) AS pools_used,
        COUNT(DISTINCT platform) AS platforms_used,
        COUNT(*) AS total_actions,
        SUM(CASE WHEN event_name IN ('Mint', 'AddLiquidity') THEN 1 ELSE 0 END) AS add_actions,
        SUM(CASE WHEN event_name IN ('Burn', 'RemoveLiquidity') THEN 1 ELSE 0 END) AS remove_actions
    FROM <blockchain_name>.defi.ez_dex_liquidity_pool_actions
    WHERE block_timestamp >= CURRENT_DATE - 30
    GROUP BY 1
)
SELECT 
    CASE 
        WHEN pools_used = 1 THEN 'Single Pool'
        WHEN pools_used <= 5 THEN 'Few Pools (2-5)'
        WHEN pools_used <= 20 THEN 'Many Pools (6-20)'
        ELSE 'Whale LP (20+)'
    END AS lp_category,
    COUNT(*) AS lp_count,
    AVG(total_actions) AS avg_actions_per_lp,
    AVG(pools_used) AS avg_pools_per_lp
FROM lp_stats
GROUP BY 1
ORDER BY 2 DESC;

-- Pool liquidity flow analysis
SELECT 
    pool_address,
    pool_name,
    platform,
    symbols:token0::string || '/' || symbols:token1::string AS pair,
    SUM(CASE WHEN event_name IN ('Mint', 'AddLiquidity') THEN 1 ELSE 0 END) AS additions,
    SUM(CASE WHEN event_name IN ('Burn', 'RemoveLiquidity') THEN 1 ELSE 0 END) AS removals,
    COUNT(DISTINCT liquidity_provider) AS unique_lps,
    COUNT(*) AS total_actions
FROM <blockchain_name>.defi.ez_dex_liquidity_pool_actions
WHERE block_timestamp >= CURRENT_DATE - 7
GROUP BY 1, 2, 3, 4
HAVING total_actions >= 10
ORDER BY total_actions DESC
LIMIT 100;

-- Platform market share by LP activity
SELECT 
    platform,
    COUNT(*) AS total_actions,
    COUNT(DISTINCT liquidity_provider) AS unique_lps,
    COUNT(DISTINCT pool_address) AS active_pools,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS action_share_pct
FROM <blockchain_name>.defi.ez_dex_liquidity_pool_actions
WHERE block_timestamp >= CURRENT_DATE - 7
GROUP BY 1
ORDER BY total_actions DESC;
```

{% enddocs %}

{% docs ez_dex_liquidity_pool_actions_liquidity_provider %}

The address that is providing or removing liquidity from the pool.

Example: '0x1234567890123456789012345678901234567890'

{% enddocs %}

{% docs ez_dex_liquidity_pool_actions_sender %}

The address that initiated the liquidity pool action function.

Example: '0x7a250d5630b4cf539739df2c5dacb4c659f2488d'

{% enddocs %}

{% docs ez_dex_liquidity_pool_actions_receiver %}

The recipient address of the LP tokens or withdrawn assets.

Example: '0x1234567890123456789012345678901234567890'

{% enddocs %}

{% docs ez_dex_liquidity_pool_actions_amounts_unadj %}

JSON object containing raw, non-decimal adjusted amounts for each token in the liquidity action. Can contain up to 8 tokens (token0-token7) depending on the pool type.

Example: {"token0": "1000500000000000000000", "token1": "2000000000", "token2": "500000000"}

{% enddocs %}

{% docs ez_dex_liquidity_pool_actions_amounts %}

JSON object containing decimal-adjusted amounts for each token in the liquidity action. Can contain up to 8 tokens (token0-token7) depending on the pool type.

Example: {"token0": "1000.5", "token1": "2000.0", "token2": "500.0"}

{% enddocs %}

{% docs ez_dex_liquidity_pool_actions_amounts_usd %}

JSON object containing USD values for each token in the liquidity action at time of transaction. Can contain up to 8 tokens (token0-token7) depending on the pool type.

Example: {"token0": "1500.75", "token1": "2000.00", "token2": "750.25"}

{% enddocs %}

{% docs ez_dex_liquidity_pool_actions_tokens_is_verified %}

JSON object indicating whether each token in the pool is verified. Can contain up to 8 tokens (token0-token7) depending on the pool type.

Example: {"token0": true, "token1": true, "token2": false}

{% enddocs %}

{% docs ez_dex_liquidity_pool_actions_platform %}

The DEX protocol and version where the liquidity action occurred.

Example: 'uniswap-v3'

{% enddocs %}

{% docs ez_dex_liquidity_pool_actions_protocol %}

The protocol used for the liquidity action. This is the clean name of the protocol without the version.

Example: 'uniswap'

{% enddocs %}

{% docs ez_dex_liquidity_pool_actions_protocol_version %}

The version of the protocol used for the liquidity action.

Example: 'v3'

{% enddocs %}

{% docs ez_dex_liquidity_pool_actions_pool_address %}

The liquidity pool contract address where the action occurred.

Example: '0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8'

{% enddocs %}

{% docs ez_dex_liquidity_pool_actions_pool_name %}

Human-readable name for the liquidity pool.

Example: 'WETH/USDC'

{% enddocs %}

{% docs ez_dex_liquidity_pool_actions_tokens %}

JSON object containing token contract addresses in the pool. Can contain up to 8 tokens (token0-token7) depending on the pool type and protocol (e.g., Balancer, Curve).

Example: {"token0": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", "token1": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", "token2": "0x6b175474e89094c44da98b954eedeac495271d0f"}

{% enddocs %}

{% docs ez_dex_liquidity_pool_actions_symbols %}

JSON object containing token symbols for the pool. Can contain up to 8 tokens (token0-token7) depending on the pool type and protocol (e.g., Balancer, Curve).

Example: {"token0": "WETH", "token1": "USDC", "token2": "DAI"}

{% enddocs %}

{% docs ez_dex_liquidity_pool_actions_decimals %}

JSON object containing decimal places for each token in the pool. Can contain up to 8 tokens (token0-token7) depending on the pool type and protocol (e.g., Balancer, Curve).

Example: {"token0": 18, "token1": 6, "token2": 18}

{% enddocs %}
