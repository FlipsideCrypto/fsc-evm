{% docs ez_balances_native_daily_table_doc %}

## What

This table provides daily native asset balance snapshots (ETH, AVAX, BNB, etc.) by making direct `eth_getBalance` RPC calls at the end of each day. It captures the current native asset balance for each address that has had native asset activity, providing a comprehensive view of native holdings with decimal adjustments and USD valuations where available. Historical native balances data available, starting from `2025-06-10`.

## Key Use Cases

- Daily portfolio tracking and native asset balance monitoring
- Historical native asset balance analysis and trend identification
- Whale tracking and large holder distribution analysis
- Daily balance snapshots for reporting and analytics
- Native asset concentration analysis across addresses
- Daily balance-based yield and return calculations for native assets
- End-of-day balance reconciliation and accounting

## Important Relationships

- **Join with fact_blocks**: Use `block_number` for block metadata and timestamps
- **Join with dim_labels**: Use `address` for entity identification and categorization
- **Join with ez_prices_hourly**: USD valuations already included but can be refreshed
- **Join with ez_balances_erc20_daily**: Compare with ERC20 token daily balances
- **Join with ez_native_transfers**: Compare daily balances with transfer activity
- **Join with core__fact_traces**: Analyze native asset movement patterns

## Commonly-used Fields

- `address`: The account address holding the native asset balance
- `balance`: Native asset balance at end of day, decimal adjusted to standard units
- `balance_usd`: USD value of the native asset balance at end of day
- `balance_raw`: Raw balance in smallest unit (Wei) without decimal adjustment
- `balance_precise`: Precise decimal-adjusted balance as string
- `balance_hex`: Hexadecimal balance as returned by eth_getBalance
- `decimals`: Number of decimal places (always 18 for native assets)
- `block_date`: The date for which this balance snapshot was taken

## Sample queries

**Daily Native Asset Holdings by Address**
```sql
SELECT 
    block_date,
    address,
    balance,
    balance_usd,
    LAG(balance) OVER (
        PARTITION BY address 
        ORDER BY block_date
    ) AS prev_balance,
    balance - LAG(balance) OVER (
        PARTITION BY address 
        ORDER BY block_date
    ) AS daily_change
FROM <blockchain_name>.balances.ez_balances_native_daily
WHERE address = LOWER('0x1234567890123456789012345678901234567890')
    AND block_date >= CURRENT_DATE - 30
    AND balance > 0
ORDER BY block_date DESC;
```

**Native Asset Holder Distribution Trends**
```sql
SELECT 
    block_date,
    COUNT(DISTINCT address) AS total_holders,
    COUNT(DISTINCT CASE WHEN balance >= 1 THEN address END) AS holders_1_plus,
    COUNT(DISTINCT CASE WHEN balance >= 10 THEN address END) AS holders_10_plus,
    COUNT(DISTINCT CASE WHEN balance >= 100 THEN address END) AS holders_100_plus,
    COUNT(DISTINCT CASE WHEN balance >= 1000 THEN address END) AS holders_1000_plus,
    SUM(balance) AS total_native_tracked,
    AVG(balance) AS avg_balance,
    MEDIAN(balance) AS median_balance,
    MAX(balance) AS max_balance
FROM <blockchain_name>.balances.ez_balances_native_daily
WHERE block_date >= CURRENT_DATE - 90
    AND balance > 0
GROUP BY 1
ORDER BY 1 DESC;
```

**Whale Activity Monitoring**
```sql
-- Track large native asset holders and their balance changes
WITH whale_balances AS (
    SELECT 
        address,
        block_date,
        balance,
        balance_usd,
        LAG(balance) OVER (
            PARTITION BY address 
            ORDER BY block_date
        ) AS prev_balance,
        LAG(balance_usd) OVER (
            PARTITION BY address 
            ORDER BY block_date
        ) AS prev_balance_usd
    FROM <blockchain_name>.balances.ez_balances_native_daily
    WHERE balance >= 1000  -- Focus on large holders
        AND block_date >= CURRENT_DATE - 7
),
whale_changes AS (
    SELECT 
        *,
        balance - prev_balance AS balance_change,
        balance_usd - prev_balance_usd AS balance_change_usd,
        CASE 
            WHEN prev_balance > 0 
            THEN ((balance - prev_balance) / prev_balance) * 100 
            ELSE NULL 
        END AS pct_change
    FROM whale_balances
    WHERE prev_balance IS NOT NULL
)
SELECT 
    block_date,
    address,
    balance,
    balance_change,
    balance_change_usd,
    pct_change,
    CASE 
        WHEN balance_change > 100 THEN 'Large Increase'
        WHEN balance_change > 10 THEN 'Moderate Increase'
        WHEN balance_change < -100 THEN 'Large Decrease'
        WHEN balance_change < -10 THEN 'Moderate Decrease'
        ELSE 'Stable'
    END AS change_category
FROM whale_changes
WHERE ABS(balance_change) > 5  -- Only show meaningful changes
ORDER BY ABS(balance_change_usd) DESC
LIMIT 100;
```

**Daily Balance Distribution Analysis**
```sql
-- Analyze the distribution of native asset balances
SELECT 
    block_date,
    CASE 
        WHEN balance < 0.001 THEN 'Dust (<0.001)'
        WHEN balance < 0.01 THEN 'Very Small (0.001-0.01)'
        WHEN balance < 0.1 THEN 'Small (0.01-0.1)'
        WHEN balance < 1 THEN 'Medium (0.1-1)'
        WHEN balance < 10 THEN 'Large (1-10)'
        WHEN balance < 100 THEN 'Very Large (10-100)'
        WHEN balance < 1000 THEN 'Whale (100-1000)'
        ELSE 'Super Whale (1000+)'
    END AS balance_category,
    COUNT(DISTINCT address) AS address_count,
    SUM(balance) AS total_balance,
    SUM(balance_usd) AS total_balance_usd,
    AVG(balance) AS avg_balance,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY balance) AS median_balance
FROM <blockchain_name>.balances.ez_balances_native_daily
WHERE block_date = CURRENT_DATE - 1
    AND balance > 0
GROUP BY 1, 2
ORDER BY 1 DESC, 
    CASE balance_category
        WHEN 'Dust (<0.001)' THEN 1
        WHEN 'Very Small (0.001-0.01)' THEN 2
        WHEN 'Small (0.01-0.1)' THEN 3
        WHEN 'Medium (0.1-1)' THEN 4
        WHEN 'Large (1-10)' THEN 5
        WHEN 'Very Large (10-100)' THEN 6
        WHEN 'Whale (100-1000)' THEN 7
        WHEN 'Super Whale (1000+)' THEN 8
    END;
```

**Portfolio Value Evolution**
```sql
-- Track total portfolio value changes for top holders
SELECT 
    block_date,
    COUNT(DISTINCT address) AS tracked_addresses,
    SUM(balance_usd) AS total_portfolio_value,
    AVG(balance_usd) AS avg_portfolio_value,
    SUM(balance) AS total_native_balance,
    LAG(SUM(balance_usd)) OVER (ORDER BY block_date) AS prev_total_value,
    (SUM(balance_usd) - LAG(SUM(balance_usd)) OVER (ORDER BY block_date)) AS daily_value_change,
    CASE 
        WHEN LAG(SUM(balance_usd)) OVER (ORDER BY block_date) > 0
        THEN ((SUM(balance_usd) - LAG(SUM(balance_usd)) OVER (ORDER BY block_date)) / LAG(SUM(balance_usd)) OVER (ORDER BY block_date)) * 100
        ELSE NULL
    END AS daily_pct_change
FROM <blockchain_name>.balances.ez_balances_native_daily
WHERE block_date >= CURRENT_DATE - 30
    AND balance_usd >= 10000  -- Focus on significant holders
GROUP BY 1
ORDER BY 1 DESC;
```

{% enddocs %}

{% docs ez_balances_native_daily_block_date %}

The date for which this balance snapshot represents the end-of-day native asset balance.

Example: '2023-12-15'

{% enddocs %}

{% docs ez_balances_native_daily_address %}

The account address whose native asset balance is recorded in this daily snapshot.

Example: '0x1234567890123456789012345678901234567890'

{% enddocs %}

{% docs ez_balances_native_daily_decimals %}

Number of decimal places for the native asset. Typically 18 for native EVM assets (ETH, AVAX, BNB, etc.).

Example: 18

{% enddocs %}

{% docs ez_balances_native_daily_balance_hex %}

Hexadecimal representation of the native asset balance as returned by the eth_getBalance RPC call.

Example: '0x3b9aca00'

{% enddocs %}

{% docs ez_balances_native_daily_balance_raw %}

Native asset balance in the smallest unit (Wei) without decimal adjustment, as returned by eth_getBalance.

Example: 1000000000000000000

{% enddocs %}

{% docs ez_balances_native_daily_balance_precise %}

Native asset balance with proper decimal adjustment, returned as a string to preserve precision.

Example: '1.000000000000000000'

{% enddocs %}

{% docs ez_balances_native_daily_balance %}

Native asset balance with decimal adjustment converted to a float for easier mathematical operations.

Example: 1.0

{% enddocs %}

{% docs ez_balances_native_daily_balance_usd %}

USD value of the native asset balance at the end of the day, calculated using hourly price data.

Example: 2500.75

{% enddocs %}