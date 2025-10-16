{% docs ez_dex_liquidity_pool_actions_table_doc %}

## What

This table provides a comprehensive view of liquidity pool actions across major decentralized exchanges (DEXs) on EVM blockchains. Each row represents **one token** in a liquidity pool action. For example, if a user adds WETH and USDC to a pool, there will be 2 rows: one for WETH and one for USDC.

## Key Use Cases

- Analyzing liquidity provision patterns and LP behavior
- Tracking token-specific liquidity flows
- Monitoring large liquidity additions/removals by token
- Calculating LP rewards and impermanent loss
- Identifying popular tokens in liquidity pools

## Important Relationships

- **Join with ez_dex_swaps**: Correlate LP actions with trading activity
- **Join with ez_prices_hourly**: Get historical token prices
- **Self-join on ez_dex_liquidity_pool_actions_id**: Group tokens from the same action

## Commonly-used Fields

- `platform`: DEX protocol (uniswap-v3, uniswap-v2 etc.)
- `event_name`: Type of action (Mint, Burn, AddLiquidity, RemoveLiquidity, Deposit, Withdraw etc.)
- `liquidity_provider`: Address providing/removing liquidity
- `pool_address`: Liquidity pool where action occurred
- `token_address`: Individual token in the action
- `amount`: Decimal-adjusted token amount
- `amount_usd`: USD value of the token amount

## Sample queries

```sql
-- Top tokens by liquidity additions (last 7 days)
SELECT 
    token_address,
    symbol,
    COUNT(DISTINCT tx_hash) AS add_count,
    SUM(amount_usd) AS total_usd_added
FROM <blockchain_name>.defi.ez_dex_liquidity_pool_actions
WHERE block_timestamp >= CURRENT_DATE - 7
    AND event_name IN ('Mint', 'AddLiquidity', 'Deposit')
GROUP BY 1, 2
ORDER BY total_usd_added DESC
LIMIT 50;

-- Largest single token liquidity actions
SELECT 
    block_timestamp,
    tx_hash,
    platform,
    pool_name,
    liquidity_provider,
    symbol,
    amount,
    amount_usd
FROM <blockchain_name>.defi.ez_dex_liquidity_pool_actions
WHERE block_timestamp >= CURRENT_DATE - 7
    AND amount_usd > 0
ORDER BY amount_usd DESC
LIMIT 100;

-- Daily LP activity by platform
SELECT 
    DATE_TRUNC('day', block_timestamp) AS date,
    platform,
    COUNT(DISTINCT liquidity_provider) AS unique_lps,
    COUNT(DISTINCT pool_address) AS active_pools,
    SUM(amount_usd) AS total_volume_usd
FROM <blockchain_name>.defi.ez_dex_liquidity_pool_actions
WHERE block_timestamp >= CURRENT_DATE - 30
GROUP BY 1, 2
ORDER BY 1 DESC, 5 DESC;
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

{% docs ez_dex_liquidity_pool_actions_token_address %}

The contract address of the individual token in this liquidity pool action.

Example: '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'

{% enddocs %}

{% docs ez_dex_liquidity_pool_actions_symbol %}

The symbol of the individual token.

Example: 'WETH'

{% enddocs %}

{% docs ez_dex_liquidity_pool_actions_decimals %}

The number of decimal places for the individual token.

Example: 18

{% enddocs %}

{% docs ez_dex_liquidity_pool_actions_amount_unadj %}

Raw, non-decimal adjusted amount of the individual token in this action.

Example: 1000500000000000000000

{% enddocs %}

{% docs ez_dex_liquidity_pool_actions_amount %}

Decimal-adjusted amount of the individual token in this action.

Example: 1000.5

{% enddocs %}

{% docs ez_dex_liquidity_pool_actions_amount_usd %}

USD value of the individual token amount at the time of the transaction.

Example: 1500.75

{% enddocs %}

{% docs ez_dex_liquidity_pool_actions_token_is_verified %}

Whether the individual token is verified in the Flipside token metadata.

Example: true

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

