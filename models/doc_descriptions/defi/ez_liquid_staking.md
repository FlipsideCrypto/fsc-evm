{% docs ez_liquid_staking_deposits_table_doc %}

## What

This table provides comprehensive tracking of liquid staking deposits across major liquid staking derivative (LSD) protocols on Ethereum. It captures when users stake ETH and receive liquid staking tokens in return, enabling analysis of staking adoption, protocol market share, and capital flows into the liquid staking ecosystem.

## Key Use Cases

- Tracking liquid staking adoption and growth trends
- Analyzing protocol market share and competitive dynamics
- Understanding staker behavior and deposit patterns
- Monitoring large deposits and whale activity
- Calculating exchange rates between ETH and LSD tokens

## Important Relationships

- Links to `core.fact_event_logs` via `tx_hash` and `event_index`
- Joins with `ez_liquid_staking_withdrawals` for full lifecycle tracking
- References `core.dim_contracts` for protocol metadata
- Connects to `price.ez_prices_hourly` for USD conversions

## Commonly-used Fields

- `staker`: Address performing the staking action
- `platform`: Liquid staking protocol name
- `eth_amount`: Amount of ETH staked
- `token_amount`: LSD tokens received
- `token_symbol`: Symbol of the LSD token (stETH, rETH, etc.)
- `block_timestamp`: When the deposit occurred

## Sample queries

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

{% enddocs %}

{% docs ez_liquid_staking_withdrawals_table_doc %}

## What

This table tracks liquid staking withdrawals/unstaking events across major LSD protocols. It captures when users burn their liquid staking tokens to reclaim ETH, providing insights into unstaking patterns, liquidity needs, and protocol exit flows.

## Key Use Cases

- Monitoring withdrawal volumes and exit liquidity
- Analyzing net staking flows (deposits minus withdrawals)
- Understanding staker holding periods and behavior
- Detecting large withdrawals and de-risking events
- Tracking exchange rates at withdrawal time

## Important Relationships

- Links to `ez_liquid_staking_deposits` for position lifecycle analysis
- Connects to `core.fact_event_logs` for transaction details
- References withdrawal queue contracts for protocols with exit delays
- Uses `price.ez_prices_hourly` for USD valuations

## Commonly-used Fields

- `staker`: Address performing the unstaking
- `platform`: Liquid staking protocol name
- `eth_amount`: Amount of ETH received
- `token_amount`: LSD tokens burned
- `token_symbol`: Symbol of the LSD token
- `block_timestamp`: When withdrawal occurred

## Sample queries

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

{% enddocs %}

{% docs ez_liquid_staking_staker %}

The address performing the staking or unstaking action.

Example: '0x1234567890123456789012345678901234567890'

{% enddocs %}

{% docs ez_liquid_staking_platform %}

The liquid staking protocol processing the transaction.

Example: 'lido'

{% enddocs %}

{% docs ez_liquid_staking_token_symbol %}

The symbol of the liquid staking derivative token.

Example: 'stETH'

{% enddocs %}

{% docs ez_liquid_staking_token_address %}

The contract address of the liquid staking token.

Example: '0xae7ab96520de3a18e5e111b5eaab095312d7fe84'

{% enddocs %}

{% docs ez_liquid_staking_eth_amount_unadj %}

The raw amount of ETH without decimal adjustment.

Example: 1000000000000000000

{% enddocs %}

{% docs ez_liquid_staking_eth_amount %}

The decimal-adjusted amount of ETH staked or withdrawn.

Example: 1.0

{% enddocs %}

{% docs ez_liquid_staking_eth_amount_usd %}

The USD value of ETH staked or withdrawn.

Example: 2500.50

{% enddocs %}

{% docs ez_liquid_staking_token_amount_unadj %}

The raw amount of liquid staking tokens without decimal adjustment.

Example: 999500000000000000

{% enddocs %}

{% docs ez_liquid_staking_token_amount %}

The decimal-adjusted amount of liquid staking tokens minted or burned.

Example: 0.9995

{% enddocs %}

{% docs ez_liquid_staking_token_amount_usd %}

The USD value of liquid staking tokens minted or burned.

Example: 2498.75

{% enddocs %}