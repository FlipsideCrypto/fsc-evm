{% docs ez_liquid_staking_deposits_table_doc %}

## Overview
This table provides comprehensive tracking of liquid staking deposits across major liquid staking derivative (LSD) protocols on Ethereum. It captures when users stake ETH and receive liquid staking tokens in return, enabling analysis of staking adoption, protocol market share, and capital flows into the liquid staking ecosystem.

### Key Features
- **Multi-protocol coverage**: Includes Lido, Rocket Pool, Coinbase, Frax, and 13+ other major protocols
- **Token minting events**: Tracks LSD token issuance (stETH, rETH, cbETH, etc.)
- **USD valuations**: Converts both ETH deposited and tokens received to USD values
- **Exchange rate tracking**: Enables analysis of token:ETH ratios over time

### Important Relationships
- Links to `core.fact_event_logs` via `tx_hash` and `event_index`
- Joins with `ez_liquid_staking_withdrawals` for full lifecycle tracking
- References `core.dim_contracts` for protocol metadata
- Connects to `price.ez_prices_hourly` for USD conversions

### Sample Queries

```sql
-- Daily liquid staking deposits by protocol
SELECT 
    DATE_TRUNC('day', block_timestamp) AS date,
    platform,
    COUNT(DISTINCT tx_hash) AS deposit_txns,
    COUNT(DISTINCT staker) AS unique_stakers,
    SUM(eth_amount) AS eth_staked,
    SUM(eth_amount_usd) AS usd_staked,
    AVG(eth_amount) AS avg_stake_size
FROM defi.ez_liquid_staking_deposits
WHERE block_timestamp >= CURRENT_DATE - 30
    AND eth_amount_usd IS NOT NULL
GROUP BY 1, 2
ORDER BY 1 DESC, 6 DESC;

-- Protocol market share analysis
WITH protocol_totals AS (
    SELECT 
        platform,
        SUM(eth_amount) AS total_eth_staked,
        COUNT(DISTINCT staker) AS unique_stakers,
        COUNT(*) AS total_deposits
    FROM defi.ez_liquid_staking_deposits
    WHERE block_timestamp >= CURRENT_DATE - 90
        AND eth_amount IS NOT NULL
    GROUP BY platform
)
SELECT 
    platform,
    total_eth_staked,
    total_eth_staked * 100.0 / SUM(total_eth_staked) OVER () AS market_share_pct,
    unique_stakers,
    total_deposits,
    total_eth_staked / total_deposits AS avg_deposit_size
FROM protocol_totals
ORDER BY total_eth_staked DESC;

-- Staker behavior patterns
WITH staker_activity AS (
    SELECT 
        staker,
        COUNT(DISTINCT platform) AS protocols_used,
        COUNT(*) AS total_deposits,
        SUM(eth_amount) AS total_eth_staked,
        MIN(block_timestamp) AS first_stake,
        MAX(block_timestamp) AS last_stake,
        COUNT(DISTINCT DATE_TRUNC('month', block_timestamp)) AS active_months
    FROM defi.ez_liquid_staking_deposits
    WHERE eth_amount IS NOT NULL
    GROUP BY staker
)
SELECT 
    CASE 
        WHEN total_eth_staked < 1 THEN '< 1 ETH'
        WHEN total_eth_staked < 10 THEN '1-10 ETH'
        WHEN total_eth_staked < 32 THEN '10-32 ETH'
        WHEN total_eth_staked < 100 THEN '32-100 ETH'
        ELSE '100+ ETH'
    END AS staker_tier,
    COUNT(*) AS staker_count,
    AVG(total_deposits) AS avg_deposits_per_staker,
    AVG(protocols_used) AS avg_protocols_used,
    SUM(total_eth_staked) AS tier_total_eth
FROM staker_activity
GROUP BY staker_tier
ORDER BY MIN(total_eth_staked);

-- Exchange rate analysis (token received per ETH)
SELECT 
    platform,
    token_symbol,
    DATE_TRUNC('day', block_timestamp) AS date,
    AVG(token_amount / NULLIF(eth_amount, 0)) AS avg_exchange_rate,
    MIN(token_amount / NULLIF(eth_amount, 0)) AS min_rate,
    MAX(token_amount / NULLIF(eth_amount, 0)) AS max_rate,
    COUNT(*) AS sample_size
FROM defi.ez_liquid_staking_deposits
WHERE eth_amount > 0 
    AND token_amount > 0
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY 1, 2, 3
ORDER BY 1, 3 DESC;

-- Large deposits monitoring (whale activity)
SELECT 
    block_timestamp,
    tx_hash,
    platform,
    staker,
    eth_amount,
    eth_amount_usd,
    token_symbol,
    token_amount,
    token_amount / NULLIF(eth_amount, 0) AS exchange_rate
FROM defi.ez_liquid_staking_deposits
WHERE eth_amount >= 100
    AND block_timestamp >= CURRENT_DATE - 7
ORDER BY eth_amount DESC;

-- Weekly staking momentum
WITH weekly_deposits AS (
    SELECT 
        DATE_TRUNC('week', block_timestamp) AS week,
        platform,
        SUM(eth_amount) AS weekly_eth_staked,
        COUNT(DISTINCT staker) AS unique_stakers
    FROM defi.ez_liquid_staking_deposits
    WHERE block_timestamp >= CURRENT_DATE - 84
        AND eth_amount IS NOT NULL
    GROUP BY 1, 2
)
SELECT 
    week,
    platform,
    weekly_eth_staked,
    LAG(weekly_eth_staked) OVER (PARTITION BY platform ORDER BY week) AS prev_week_eth,
    (weekly_eth_staked / NULLIF(LAG(weekly_eth_staked) OVER (PARTITION BY platform ORDER BY week), 0) - 1) * 100 AS week_over_week_pct,
    unique_stakers
FROM weekly_deposits
ORDER BY week DESC, weekly_eth_staked DESC;
```

### Critical Usage Notes
- **Beacon Chain limitation**: Only tracks onchain LSD events, not direct validator deposits
- **Protocol coverage**: New protocols added as they gain traction
- **Exchange rates**: Token:ETH ratios vary by protocol mechanism (rebasing vs reward-bearing)
- **USD values**: May be NULL during price feed outages
- **Performance tip**: Always filter by `block_timestamp` for large queries

### Data Quality Considerations
- Some protocols use wrapper contracts that may show as intermediary addresses
- Exchange rates can temporarily deviate during high demand periods
- Minimum deposit amounts vary by protocol (some have no minimum)
- Protocol migrations may show as deposits to new contracts

{% enddocs %}

{% docs ez_liquid_staking_withdrawals_table_doc %}

## Overview
This table tracks liquid staking withdrawals/unstaking events across major LSD protocols. It captures when users burn their liquid staking tokens to reclaim ETH, providing insights into unstaking patterns, liquidity needs, and protocol exit flows.

### Key Features
- **Unstaking tracking**: Records burn/redeem events for liquid staking tokens
- **Exit liquidity**: Monitors ETH redemption volumes and timing
- **Multi-protocol**: Covers 11+ protocols including Lido, Rocket Pool, Coinbase
- **Value preservation**: Tracks both ETH received and tokens burned with USD values

### Important Relationships
- Links to `ez_liquid_staking_deposits` for position lifecycle analysis
- Connects to `core.fact_event_logs` for transaction details
- References withdrawal queue contracts for protocols with exit delays
- Uses `price.ez_prices_hourly` for USD valuations

### Sample Queries

```sql
-- Daily withdrawal patterns by protocol
SELECT 
    DATE_TRUNC('day', block_timestamp) AS date,
    platform,
    COUNT(*) AS withdrawal_txns,
    COUNT(DISTINCT staker) AS unique_unstakers,
    SUM(eth_amount) AS eth_withdrawn,
    SUM(eth_amount_usd) AS usd_withdrawn,
    AVG(eth_amount) AS avg_withdrawal_size
FROM defi.ez_liquid_staking_withdrawals
WHERE block_timestamp >= CURRENT_DATE - 30
    AND eth_amount_usd IS NOT NULL
GROUP BY 1, 2
ORDER BY 1 DESC, 5 DESC;

-- Net staking flows (deposits vs withdrawals)
WITH daily_deposits AS (
    SELECT 
        DATE_TRUNC('day', block_timestamp) AS date,
        platform,
        SUM(eth_amount) AS eth_deposited,
        COUNT(DISTINCT staker) AS depositors
    FROM defi.ez_liquid_staking_deposits
    WHERE block_timestamp >= CURRENT_DATE - 30
        AND eth_amount IS NOT NULL
    GROUP BY 1, 2
),
daily_withdrawals AS (
    SELECT 
        DATE_TRUNC('day', block_timestamp) AS date,
        platform,
        SUM(eth_amount) AS eth_withdrawn,
        COUNT(DISTINCT staker) AS withdrawers
    FROM defi.ez_liquid_staking_withdrawals
    WHERE block_timestamp >= CURRENT_DATE - 30
        AND eth_amount IS NOT NULL
    GROUP BY 1, 2
)
SELECT 
    COALESCE(d.date, w.date) AS date,
    COALESCE(d.platform, w.platform) AS platform,
    COALESCE(d.eth_deposited, 0) AS eth_deposited,
    COALESCE(w.eth_withdrawn, 0) AS eth_withdrawn,
    COALESCE(d.eth_deposited, 0) - COALESCE(w.eth_withdrawn, 0) AS net_eth_flow,
    COALESCE(d.depositors, 0) AS depositors,
    COALESCE(w.withdrawers, 0) AS withdrawers
FROM daily_deposits d
FULL OUTER JOIN daily_withdrawals w 
    ON d.date = w.date AND d.platform = w.platform
ORDER BY date DESC, ABS(net_eth_flow) DESC;

-- Staker holding period analysis
WITH staker_lifecycle AS (
    SELECT 
        d.staker,
        d.platform,
        d.block_timestamp AS deposit_time,
        MIN(w.block_timestamp) AS withdrawal_time,
        d.eth_amount AS deposit_amount,
        d.token_amount AS tokens_received
    FROM defi.ez_liquid_staking_deposits d
    LEFT JOIN defi.ez_liquid_staking_withdrawals w
        ON d.staker = w.staker 
        AND d.platform = w.platform
        AND d.token_address = w.token_address
        AND w.block_timestamp > d.block_timestamp
    WHERE d.eth_amount IS NOT NULL
    GROUP BY 1, 2, 3, 5, 6
)
SELECT 
    platform,
    COUNT(CASE WHEN withdrawal_time IS NOT NULL THEN 1 END) AS completed_cycles,
    COUNT(CASE WHEN withdrawal_time IS NULL THEN 1 END) AS still_staking,
    AVG(CASE 
        WHEN withdrawal_time IS NOT NULL 
        THEN EXTRACT(EPOCH FROM (withdrawal_time - deposit_time)) / 86400 
    END) AS avg_holding_days,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE 
            WHEN withdrawal_time IS NOT NULL 
            THEN EXTRACT(EPOCH FROM (withdrawal_time - deposit_time)) / 86400 
        END
    ) AS median_holding_days
FROM staker_lifecycle
WHERE deposit_time >= CURRENT_DATE - 365
GROUP BY platform
ORDER BY completed_cycles DESC;

-- Exchange rate at withdrawal (profit/loss analysis)
SELECT 
    platform,
    token_symbol,
    DATE_TRUNC('week', block_timestamp) AS week,
    AVG(eth_amount / NULLIF(token_amount, 0)) AS avg_redemption_rate,
    MIN(eth_amount / NULLIF(token_amount, 0)) AS min_rate,
    MAX(eth_amount / NULLIF(token_amount, 0)) AS max_rate,
    COUNT(*) AS withdrawals
FROM defi.ez_liquid_staking_withdrawals
WHERE token_amount > 0 
    AND eth_amount > 0
    AND block_timestamp >= CURRENT_DATE - 90
GROUP BY 1, 2, 3
ORDER BY 1, 3 DESC;

-- Large withdrawals monitoring (potential de-risking)
SELECT 
    block_timestamp,
    tx_hash,
    platform,
    staker,
    eth_amount,
    eth_amount_usd,
    token_symbol,
    token_amount,
    eth_amount / NULLIF(token_amount, 0) AS redemption_rate
FROM defi.ez_liquid_staking_withdrawals
WHERE eth_amount >= 100
    AND block_timestamp >= CURRENT_DATE - 3
ORDER BY eth_amount DESC;

-- Withdrawal pressure indicators
WITH hourly_flows AS (
    SELECT 
        DATE_TRUNC('hour', block_timestamp) AS hour,
        platform,
        SUM(eth_amount) AS hourly_withdrawals,
        COUNT(*) AS withdrawal_count,
        COUNT(DISTINCT staker) AS unique_withdrawers
    FROM defi.ez_liquid_staking_withdrawals
    WHERE block_timestamp >= CURRENT_DATE - 7
        AND eth_amount IS NOT NULL
    GROUP BY 1, 2
)
SELECT 
    platform,
    MAX(hourly_withdrawals) AS peak_hourly_withdrawal,
    AVG(hourly_withdrawals) AS avg_hourly_withdrawal,
    MAX(withdrawal_count) AS peak_withdrawal_count,
    STDDEV(hourly_withdrawals) AS withdrawal_volatility
FROM hourly_flows
GROUP BY platform
HAVING MAX(hourly_withdrawals) > 100
ORDER BY peak_hourly_withdrawal DESC;
```

### Critical Usage Notes
- **Withdrawal delays**: Some protocols have unbonding periods not reflected in this data
- **Partial withdrawals**: Users may withdraw portions of their staked position
- **Protocol differences**: Withdrawal mechanisms vary (instant vs queued)
- **Slashing risk**: Withdrawn amounts may be less than deposited due to penalties
- **Performance tip**: Join with deposits table carefully due to multiple deposit/withdrawal cycles

### Data Quality Considerations
- Withdrawal queues may cause delays between request and execution
- Some protocols use claim processes that appear as separate transactions
- Exchange rates at withdrawal include accumulated rewards
- Emergency withdrawals may bypass normal processes

{% enddocs %}

{% docs ez_liquid_staking_staker %}

## Staker
The address performing the staking or unstaking action.

### Details
- **Type**: `VARCHAR(42)`
- **Format**: `0x` prefixed Ethereum address
- **Pattern**: Usually EOAs, occasionally smart contract wallets

### Usage Examples
```sql
-- Staker loyalty analysis
WITH staker_stats AS (
    SELECT 
        staker,
        MIN(block_timestamp) AS first_stake,
        MAX(block_timestamp) AS last_activity,
        COUNT(DISTINCT platform) AS platforms_used,
        SUM(CASE WHEN eth_amount IS NOT NULL THEN eth_amount ELSE 0 END) AS total_staked,
        COUNT(*) AS total_transactions
    FROM (
        SELECT staker, platform, eth_amount, block_timestamp 
        FROM defi.ez_liquid_staking_deposits
        UNION ALL
        SELECT staker, platform, -eth_amount AS eth_amount, block_timestamp 
        FROM defi.ez_liquid_staking_withdrawals
    ) combined
    GROUP BY staker
)
SELECT 
    CASE 
        WHEN platforms_used = 1 THEN 'Single Protocol'
        WHEN platforms_used = 2 THEN 'Two Protocols'
        ELSE 'Multi-Protocol'
    END AS staker_type,
    COUNT(*) AS staker_count,
    AVG(total_staked) AS avg_net_staked,
    AVG(EXTRACT(EPOCH FROM (last_activity - first_stake)) / 86400) AS avg_active_days
FROM staker_stats
WHERE total_transactions > 2
GROUP BY staker_type;

-- Whale staker identification
SELECT 
    staker,
    SUM(eth_amount) AS total_eth_staked,
    COUNT(DISTINCT platform) AS protocols_used,
    COUNT(*) AS deposit_count,
    ARRAY_AGG(DISTINCT platform) AS platforms,
    MAX(block_timestamp) AS last_stake_date
FROM defi.ez_liquid_staking_deposits
WHERE eth_amount IS NOT NULL
GROUP BY staker
HAVING SUM(eth_amount) >= 1000
ORDER BY total_eth_staked DESC;
```

### Notes
- Same address may use multiple protocols
- Contract wallets indicate institutional or integration usage
- High-value stakers often diversify across protocols

{% enddocs %}

{% docs ez_liquid_staking_platform %}

## Platform
The liquid staking protocol processing the transaction.

### Details
- **Type**: `VARCHAR`
- **Common values**: `'lido'`, `'rocketpool'`, `'coinbase'`, `'frax'`, `'ankr'`, `'stafi'`
- **Case**: Lowercase standardized names

### Usage Examples
```sql
-- Platform growth comparison
WITH monthly_metrics AS (
    SELECT 
        DATE_TRUNC('month', block_timestamp) AS month,
        platform,
        SUM(eth_amount) AS monthly_volume,
        COUNT(DISTINCT staker) AS unique_stakers,
        COUNT(*) AS transactions
    FROM defi.ez_liquid_staking_deposits
    WHERE eth_amount IS NOT NULL
    GROUP BY 1, 2
)
SELECT 
    platform,
    month,
    monthly_volume,
    SUM(monthly_volume) OVER (PARTITION BY platform ORDER BY month) AS cumulative_volume,
    unique_stakers,
    monthly_volume / NULLIF(LAG(monthly_volume) OVER (PARTITION BY platform ORDER BY month), 0) - 1 AS month_over_month_growth
FROM monthly_metrics
WHERE month >= CURRENT_DATE - INTERVAL '6 months'
ORDER BY platform, month DESC;

-- Platform efficiency metrics
SELECT 
    d.platform,
    COUNT(DISTINCT d.staker) AS total_stakers,
    SUM(d.eth_amount) AS total_deposited,
    SUM(w.eth_amount) AS total_withdrawn,
    (SUM(d.eth_amount) - COALESCE(SUM(w.eth_amount), 0)) AS net_staked,
    AVG(d.token_amount / NULLIF(d.eth_amount, 0)) AS avg_mint_rate
FROM defi.ez_liquid_staking_deposits d
LEFT JOIN defi.ez_liquid_staking_withdrawals w
    ON d.platform = w.platform
WHERE d.eth_amount > 0
GROUP BY d.platform
ORDER BY net_staked DESC;
```

### Notes
- Platform names are consistent across deposits and withdrawals
- New platforms added as they reach significant volume
- Some platforms have multiple versions or implementations

{% enddocs %}

{% docs ez_liquid_staking_token_symbol %}

## Token Symbol
The symbol of the liquid staking derivative token.

### Details
- **Type**: `VARCHAR`
- **Common values**: `'stETH'`, `'rETH'`, `'cbETH'`, `'frxETH'`, `'ankrETH'`
- **Purpose**: Human-readable token identifier

### Usage Examples
```sql
-- Token market share by volume
SELECT 
    token_symbol,
    SUM(eth_amount) AS total_eth_locked,
    COUNT(DISTINCT staker) AS unique_holders,
    COUNT(*) AS total_mints,
    AVG(token_amount / NULLIF(eth_amount, 0)) AS avg_mint_ratio
FROM defi.ez_liquid_staking_deposits
WHERE eth_amount > 0
    AND token_amount > 0
    AND block_timestamp >= CURRENT_DATE - 90
GROUP BY token_symbol
ORDER BY total_eth_locked DESC;

-- Token velocity analysis
WITH token_flows AS (
    SELECT 
        token_symbol,
        'deposit' AS flow_type,
        block_timestamp,
        token_amount
    FROM defi.ez_liquid_staking_deposits
    WHERE token_amount > 0
    
    UNION ALL
    
    SELECT 
        token_symbol,
        'withdrawal' AS flow_type,
        block_timestamp,
        token_amount
    FROM defi.ez_liquid_staking_withdrawals
    WHERE token_amount > 0
)
SELECT 
    token_symbol,
    DATE_TRUNC('week', block_timestamp) AS week,
    SUM(CASE WHEN flow_type = 'deposit' THEN token_amount ELSE 0 END) AS tokens_minted,
    SUM(CASE WHEN flow_type = 'withdrawal' THEN token_amount ELSE 0 END) AS tokens_burned,
    SUM(CASE WHEN flow_type = 'deposit' THEN token_amount ELSE -token_amount END) AS net_supply_change
FROM token_flows
WHERE block_timestamp >= CURRENT_DATE - 90
GROUP BY 1, 2
ORDER BY 1, 2 DESC;
```

### Notes
- Token symbols are standardized across platforms
- Some protocols have multiple token versions
- Rebasing tokens (like stETH) vs reward-bearing tokens (like rETH)

{% enddocs %}

{% docs ez_liquid_staking_token_address %}

## Token Address
The contract address of the liquid staking token.

### Details
- **Type**: `VARCHAR(42)`
- **Format**: `0x` prefixed Ethereum address
- **Purpose**: Unique token identifier

### Usage Examples
```sql
-- Token contract analysis
SELECT 
    token_address,
    token_symbol,
    platform,
    COUNT(*) AS total_events,
    COUNT(DISTINCT staker) AS unique_users,
    MIN(block_timestamp) AS first_event,
    MAX(block_timestamp) AS last_event
FROM defi.ez_liquid_staking_deposits
GROUP BY 1, 2, 3
ORDER BY total_events DESC;

-- Multi-token platform analysis
SELECT 
    platform,
    COUNT(DISTINCT token_address) AS token_versions,
    ARRAY_AGG(DISTINCT token_symbol) AS token_symbols,
    ARRAY_AGG(DISTINCT token_address) AS token_addresses
FROM defi.ez_liquid_staking_deposits
GROUP BY platform
HAVING COUNT(DISTINCT token_address) > 1
ORDER BY token_versions DESC;
```

### Notes
- Token addresses are immutable contract references
- Some platforms have upgraded token contracts over time
- Verify token addresses when integrating with protocols

{% enddocs %}

{% docs ez_liquid_staking_eth_amount_unadj %}

## ETH Amount Unadjusted
The raw amount of ETH without decimal adjustment.

### Details
- **Type**: `NUMERIC`
- **Scale**: Wei (10^18 = 1 ETH)
- **Usage**: Raw blockchain values

### Usage Examples
```sql
-- Verify decimal adjustment
SELECT 
    platform,
    AVG(eth_amount_unadj / POWER(10, 18) - eth_amount) AS adjustment_diff,
    COUNT(*) AS sample_size
FROM defi.ez_liquid_staking_deposits
WHERE eth_amount > 0
    AND eth_amount_unadj > 0
GROUP BY platform
HAVING ABS(AVG(eth_amount_unadj / POWER(10, 18) - eth_amount)) > 0.000001;
```

{% enddocs %}

{% docs ez_liquid_staking_eth_amount %}

## ETH Amount
The decimal-adjusted amount of ETH staked or withdrawn.

### Details
- **Type**: `NUMERIC`
- **Precision**: 18 decimal places
- **Common values**: 32 ETH (validator amount), various for liquid staking

### Usage Examples
```sql
-- Staking amount distribution
SELECT 
    platform,
    CASE 
        WHEN eth_amount < 1 THEN '< 1 ETH'
        WHEN eth_amount < 10 THEN '1-10 ETH'
        WHEN eth_amount < 32 THEN '10-32 ETH'
        WHEN eth_amount = 32 THEN '32 ETH (Validator)'
        WHEN eth_amount < 100 THEN '32-100 ETH'
        ELSE '100+ ETH'
    END AS amount_tier,
    COUNT(*) AS transaction_count,
    SUM(eth_amount) AS total_eth,
    AVG(eth_amount) AS avg_amount
FROM defi.ez_liquid_staking_deposits
WHERE eth_amount > 0
GROUP BY 1, 2
ORDER BY 1, MIN(eth_amount);

-- Daily staking velocity
SELECT 
    DATE_TRUNC('day', block_timestamp) AS date,
    SUM(eth_amount) AS daily_staked_eth,
    COUNT(*) AS deposit_count,
    COUNT(DISTINCT staker) AS unique_stakers,
    MAX(eth_amount) AS largest_deposit,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY eth_amount) AS median_deposit
FROM defi.ez_liquid_staking_deposits
WHERE eth_amount > 0
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY date
ORDER BY date DESC;
```

### Notes
- Minimum amounts vary by protocol
- 32 ETH deposits may indicate validator-related activity
- Large deposits often split across multiple transactions

{% enddocs %}

{% docs ez_liquid_staking_eth_amount_usd %}

## ETH Amount USD
The USD value of ETH staked or withdrawn.

### Details
- **Type**: `NUMERIC`
- **Source**: Historical ETH/USD price at block time
- **NULL cases**: Missing price data

### Usage Examples
```sql
-- USD volume trends
SELECT 
    DATE_TRUNC('week', block_timestamp) AS week,
    platform,
    SUM(eth_amount_usd) AS weekly_volume_usd,
    AVG(eth_amount_usd / NULLIF(eth_amount, 0)) AS avg_eth_price,
    COUNT(*) AS transactions
FROM defi.ez_liquid_staking_deposits
WHERE eth_amount_usd IS NOT NULL
    AND eth_amount > 0
    AND block_timestamp >= CURRENT_DATE - 180
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;

-- Price impact on staking behavior
WITH daily_metrics AS (
    SELECT 
        DATE_TRUNC('day', block_timestamp) AS date,
        AVG(eth_amount_usd / NULLIF(eth_amount, 0)) AS eth_price,
        SUM(eth_amount) AS daily_staked,
        COUNT(DISTINCT staker) AS unique_stakers
    FROM defi.ez_liquid_staking_deposits
    WHERE eth_amount_usd IS NOT NULL
        AND eth_amount > 0
        AND block_timestamp >= CURRENT_DATE - 90
    GROUP BY date
)
SELECT 
    date,
    eth_price,
    daily_staked,
    unique_stakers,
    CORR(eth_price, daily_staked) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS price_volume_correlation
FROM daily_metrics
ORDER BY date DESC;
```

### Notes
- USD values calculated at transaction time
- Useful for portfolio analysis and reporting
- Price volatility affects staking patterns

{% enddocs %}

{% docs ez_liquid_staking_token_amount_unadj %}

## Token Amount Unadjusted
The raw amount of liquid staking tokens without decimal adjustment.

### Details
- **Type**: `NUMERIC`
- **Scale**: Varies by token (usually 10^18)
- **Usage**: Raw event log values

### Usage Examples
```sql
-- Token decimal verification
SELECT 
    platform,
    token_symbol,
    LOG(10, AVG(token_amount_unadj::FLOAT / NULLIF(token_amount::FLOAT, 0))) AS implied_decimals,
    COUNT(*) AS sample_size
FROM defi.ez_liquid_staking_deposits
WHERE token_amount > 0
    AND token_amount_unadj > 0
GROUP BY 1, 2
HAVING COUNT(*) > 100;
```

{% enddocs %}

{% docs ez_liquid_staking_token_amount %}

## Token Amount
The decimal-adjusted amount of liquid staking tokens minted or burned.

### Details
- **Type**: `NUMERIC`
- **Relationship**: Usually 1:1 with ETH at mint, varies at withdrawal
- **Precision**: Token-specific decimals

### Usage Examples
```sql
-- Exchange rate analysis over time
SELECT 
    platform,
    token_symbol,
    DATE_TRUNC('day', block_timestamp) AS date,
    AVG(token_amount / NULLIF(eth_amount, 0)) AS avg_mint_rate,
    STDDEV(token_amount / NULLIF(eth_amount, 0)) AS rate_volatility,
    COUNT(*) AS daily_mints
FROM defi.ez_liquid_staking_deposits
WHERE eth_amount > 0
    AND token_amount > 0
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY 1, 2, 3
ORDER BY 1, 3 DESC;

-- Token supply tracking
WITH token_supply_changes AS (
    SELECT 
        token_symbol,
        block_timestamp,
        token_amount AS supply_change
    FROM defi.ez_liquid_staking_deposits
    WHERE token_amount > 0
    
    UNION ALL
    
    SELECT 
        token_symbol,
        block_timestamp,
        -token_amount AS supply_change
    FROM defi.ez_liquid_staking_withdrawals
    WHERE token_amount > 0
)
SELECT 
    token_symbol,
    DATE_TRUNC('week', block_timestamp) AS week,
    SUM(supply_change) AS net_supply_change,
    SUM(SUM(supply_change)) OVER (PARTITION BY token_symbol ORDER BY DATE_TRUNC('week', block_timestamp)) AS cumulative_supply
FROM token_supply_changes
WHERE block_timestamp >= CURRENT_DATE - 90
GROUP BY 1, 2
ORDER BY 1, 2 DESC;
```

### Notes
- Rebasing tokens maintain 1:1 peg through balance updates
- Reward-bearing tokens appreciate in value over time
- Exchange rates reflect staking rewards accumulation

{% enddocs %}

{% docs ez_liquid_staking_token_amount_usd %}

## Token Amount USD
The USD value of liquid staking tokens minted or burned.

### Details
- **Type**: `NUMERIC`
- **Calculation**: Token amount × token price at block time
- **NULL cases**: Missing token price data

### Usage Examples
```sql
-- Token valuation comparison
SELECT 
    token_symbol,
    platform,
    AVG(token_amount_usd / NULLIF(eth_amount_usd, 0)) AS avg_token_eth_ratio,
    MIN(token_amount_usd / NULLIF(eth_amount_usd, 0)) AS min_ratio,
    MAX(token_amount_usd / NULLIF(eth_amount_usd, 0)) AS max_ratio,
    COUNT(*) AS samples
FROM defi.ez_liquid_staking_deposits
WHERE token_amount_usd > 0
    AND eth_amount_usd > 0
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY 1, 2
ORDER BY 3 DESC;

-- Premium/discount analysis
WITH deposit_ratios AS (
    SELECT 
        platform,
        token_symbol,
        block_timestamp,
        (token_amount_usd / NULLIF(eth_amount_usd, 0) - 1) * 100 AS premium_discount_pct
    FROM defi.ez_liquid_staking_deposits
    WHERE token_amount_usd > 0
        AND eth_amount_usd > 0
        AND block_timestamp >= CURRENT_DATE - 90
)
SELECT 
    platform,
    token_symbol,
    AVG(premium_discount_pct) AS avg_premium_discount,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY premium_discount_pct) AS p25,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY premium_discount_pct) AS median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY premium_discount_pct) AS p75,
    COUNT(*) AS observations
FROM deposit_ratios
GROUP BY 1, 2
ORDER BY ABS(avg_premium_discount) DESC;
```

### Notes
- Token USD values may temporarily deviate from ETH value
- Market conditions affect token liquidity and pricing
- Useful for identifying arbitrage opportunities

{% enddocs %}