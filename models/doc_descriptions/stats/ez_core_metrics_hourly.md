{% docs ez_core_metrics_hourly_table_doc %}

## Table: ez_core_metrics_hourly

This aggregated table provides hourly blockchain metrics for high-level analysis and monitoring. It pre-calculates key statistics from fact_transactions to enable fast querying of network activity, gas usage patterns, and user behavior trends without scanning large transaction tables.

### Key Features:
- **Pre-Aggregated Metrics**: Hourly rollups for performance
- **Network Health Indicators**: Transaction success rates, block production
- **User Activity Tracking**: Unique address counts and interactions
- **Fee Analysis**: Native and USD fee totals
- **Time-Series Ready**: Optimized for trend analysis

### Common Use Cases:
- Network health dashboards
- Gas fee trend analysis
- User adoption metrics
- Blockchain comparison studies
- Activity anomaly detection

### Important Relationships:
- **Derived from fact_transactions**: All metrics aggregated from base transaction data
- **Join with ez_prices_hourly**: For native token price correlation
- **Compare across chains**: Standardized metrics enable cross-chain analysis

### Sample Queries:

**Network Health Dashboard Metrics**
```sql
-- 24-hour network health summary
WITH hourly_stats AS (
    SELECT 
        block_timestamp_hour,
        transaction_count,
        transaction_count_success,
        transaction_count_failed,
        ROUND(100.0 * transaction_count_success / NULLIF(transaction_count, 0), 2) AS success_rate,
        total_fees_native,
        total_fees_usd,
        unique_from_count AS active_users,
        block_count,
        ROUND(transaction_count::FLOAT / NULLIF(block_count, 0), 2) AS avg_tx_per_block
    FROM <blockchain_name>.stats.ez_core_metrics_hourly
    WHERE block_timestamp_hour >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
)
SELECT 
    COUNT(*) AS hours_of_data,
    SUM(transaction_count) AS total_transactions,
    AVG(success_rate) AS avg_success_rate,
    SUM(total_fees_usd) AS total_fees_usd_24h,
    SUM(active_users) AS unique_active_addresses,
    AVG(avg_tx_per_block) AS avg_tx_per_block_24h,
    MAX(transaction_count) AS peak_hourly_transactions,
    MIN(block_count) AS min_blocks_per_hour,
    MAX(block_count) AS max_blocks_per_hour
FROM hourly_stats;
```

**Gas Fee Trends Analysis**
```sql
-- Daily average gas fees with volatility
SELECT 
    DATE(block_timestamp_hour) AS date,
    AVG(total_fees_native / NULLIF(transaction_count, 0)) AS avg_fee_per_tx_native,
    AVG(total_fees_usd / NULLIF(transaction_count, 0)) AS avg_fee_per_tx_usd,
    STDDEV(total_fees_usd / NULLIF(transaction_count, 0)) AS fee_volatility_usd,
    MIN(total_fees_usd / NULLIF(transaction_count, 0)) AS min_fee_per_tx_usd,
    MAX(total_fees_usd / NULLIF(transaction_count, 0)) AS max_fee_per_tx_usd,
    SUM(transaction_count) AS daily_transactions,
    SUM(total_fees_usd) AS daily_fees_usd
FROM <blockchain_name>.stats.ez_core_metrics_hourly
WHERE block_timestamp_hour >= CURRENT_DATE - 30
    AND transaction_count > 0
GROUP BY 1
ORDER BY 1 DESC;
```

**User Activity Patterns**
```sql
-- Hourly activity patterns (UTC)
SELECT 
    EXTRACT(HOUR FROM block_timestamp_hour) AS hour_utc,
    AVG(transaction_count) AS avg_transactions,
    AVG(unique_from_count) AS avg_active_users,
    AVG(unique_to_count) AS avg_unique_recipients,
    AVG(transaction_count::FLOAT / NULLIF(unique_from_count, 0)) AS avg_tx_per_user,
    STDDEV(transaction_count) AS transaction_volatility
FROM <blockchain_name>.stats.ez_core_metrics_hourly
WHERE block_timestamp_hour >= CURRENT_DATE - 7
GROUP BY 1
ORDER BY 1;
```

**Network Congestion Detection**
```sql
-- Identify high congestion periods
WITH congestion_metrics AS (
    SELECT 
        block_timestamp_hour,
        transaction_count,
        block_count,
        transaction_count::FLOAT / NULLIF(block_count, 0) AS tx_per_block,
        total_fees_usd / NULLIF(transaction_count, 0) AS avg_fee_usd,
        transaction_count_failed::FLOAT / NULLIF(transaction_count, 0) * 100 AS failure_rate
    FROM <blockchain_name>.stats.ez_core_metrics_hourly
    WHERE block_timestamp_hour >= CURRENT_DATE - 7
),
percentiles AS (
    SELECT 
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY tx_per_block) AS p90_tx_per_block,
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY avg_fee_usd) AS p90_fee,
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY failure_rate) AS p90_failure_rate
    FROM congestion_metrics
)
SELECT 
    c.block_timestamp_hour,
    c.tx_per_block,
    c.avg_fee_usd,
    c.failure_rate,
    CASE 
        WHEN c.tx_per_block > p.p90_tx_per_block 
             AND c.avg_fee_usd > p.p90_fee THEN 'High Congestion'
        WHEN c.failure_rate > p.p90_failure_rate THEN 'Network Issues'
        ELSE 'Normal'
    END AS network_status
FROM congestion_metrics c
CROSS JOIN percentiles p
WHERE c.tx_per_block > p.p90_tx_per_block 
   OR c.avg_fee_usd > p.p90_fee
   OR c.failure_rate > p.p90_failure_rate
ORDER BY c.block_timestamp_hour DESC;
```

**Weekly Growth Metrics**
```sql
-- Week-over-week growth analysis
WITH weekly_stats AS (
    SELECT 
        DATE_TRUNC('week', block_timestamp_hour) AS week,
        SUM(transaction_count) AS weekly_transactions,
        COUNT(DISTINCT unique_from_count) AS unique_weekly_users,
        SUM(total_fees_usd) AS weekly_fees_usd,
        AVG(transaction_count_success::FLOAT / NULLIF(transaction_count, 0)) AS avg_success_rate
    FROM <blockchain_name>.stats.ez_core_metrics_hourly
    WHERE block_timestamp_hour >= CURRENT_DATE - 60
    GROUP BY 1
)
SELECT 
    week,
    weekly_transactions,
    LAG(weekly_transactions) OVER (ORDER BY week) AS prev_week_transactions,
    ROUND(100.0 * (weekly_transactions - LAG(weekly_transactions) OVER (ORDER BY week)) / 
          NULLIF(LAG(weekly_transactions) OVER (ORDER BY week), 0), 2) AS tx_growth_pct,
    unique_weekly_users,
    ROUND(100.0 * (unique_weekly_users - LAG(unique_weekly_users) OVER (ORDER BY week)) / 
          NULLIF(LAG(unique_weekly_users) OVER (ORDER BY week), 0), 2) AS user_growth_pct,
    weekly_fees_usd,
    avg_success_rate
FROM weekly_stats
ORDER BY week DESC;
```

### Data Quality Notes:
- Metrics are aggregated at hour boundaries (UTC)
- Fee precision may be limited for native totals - use tx_fee_precise in fact_transactions for exact values
- Unique counts are within each hour, not cumulative

{% enddocs %}

{% docs ez_core_metrics_hourly_hour %}

The hour boundary (UTC) for which metrics are aggregated.

**Format**: TIMESTAMP_NTZ truncated to hour
**Example**: 2024-01-15 14:00:00.000

**Usage Pattern**:
```sql
-- Join with other hourly data
JOIN <blockchain_name>.price.ez_prices_hourly
ON block_timestamp_hour = hour
```

{% enddocs %}

{% docs ez_core_metrics_hourly_block_number_min %}

The lowest block number produced within this hour.

**Usage**:
- Identify first block of the hour
- Calculate block ranges
- Detect gaps in block production

**Analysis Example**:
```sql
-- Detect block production gaps
SELECT 
    block_timestamp_hour,
    block_number_min,
    LAG(block_number_max) OVER (ORDER BY block_timestamp_hour) AS prev_hour_max,
    block_number_min - LAG(block_number_max) OVER (ORDER BY block_timestamp_hour) - 1 AS gap_size
FROM ez_core_metrics_hourly
WHERE block_timestamp_hour >= CURRENT_DATE - 1
HAVING gap_size > 0;
```

{% enddocs %}

{% docs ez_core_metrics_hourly_block_number_max %}

The highest block number produced within this hour.

**Usage**:
- Identify last block of the hour
- Calculate blocks produced per hour
- Monitor chain tip progression

{% enddocs %}

{% docs ez_core_metrics_hourly_block_count %}

Total number of blocks produced in the hour.

**Calculation**: `block_number_max - block_number_min + 1`

**Network Health Indicator**:
- Consistent count = healthy block production
- Drops may indicate consensus issues
- Varies by chain (Ethereum ~300/hour, Polygon ~1,400/hour)

{% enddocs %}

{% docs ez_core_metrics_hourly_transaction_count %}

Total number of transactions included in blocks during this hour.

**Includes**: Both successful and failed transactions
**Key Metric**: Primary indicator of network activity

**Throughput Analysis**:
```sql
-- Calculate transactions per second (TPS)
SELECT 
    block_timestamp_hour,
    transaction_count,
    ROUND(transaction_count / 3600.0, 2) AS avg_tps
FROM <blockchain_name>.stats.ez_core_metrics_hourly
WHERE block_timestamp_hour >= CURRENT_DATE - 1
ORDER BY avg_tps DESC;
```

{% enddocs %}

{% docs ez_core_metrics_hourly_count_success %}

Number of transactions that executed successfully in the hour.

**Success Criteria**: `tx_status = 'SUCCESS'` or equivalent
**Usage**: Calculate network reliability and success rates

{% enddocs %}

{% docs ez_core_metrics_hourly_transaction_count_failed %}

Number of transactions that failed or reverted in the hour.

**Common Failure Reasons**:
- Insufficient gas
- Contract reverts
- Invalid state

**Monitoring Pattern**:
```sql
-- Alert on high failure rates
SELECT *
FROM <blockchain_name>.stats.ez_core_metrics_hourly
WHERE transaction_count_failed::FLOAT / NULLIF(transaction_count, 0) > 0.10  -- >10% failure
    AND block_timestamp_hour >= CURRENT_TIMESTAMP - INTERVAL '1 hour';
```

{% enddocs %}

{% docs ez_core_metrics_hourly_unique_from_count %}

Count of distinct addresses that initiated transactions in the hour.

**Represents**: Active users/addresses
**Note**: Not cumulative - same address counted once per hour

**User Activity Metric**:
```sql
-- Daily active users
SELECT 
    DATE(block_timestamp_hour) AS date,
    SUM(unique_from_count) AS active_addresses
FROM <blockchain_name>.stats.ez_core_metrics_hourly
GROUP BY 1
ORDER BY 1 DESC;
```

{% enddocs %}

{% docs ez_core_metrics_hourly_unique_to_count %}

Count of distinct addresses that received transactions in the hour.

**Includes**:
- EOA recipients
- Contract addresses
- NULL (contract deployments)

**Usage**: Measure interaction breadth and contract activity

{% enddocs %}

{% docs ez_core_metrics_hourly_total_fees_native %}

Sum of all transaction fees paid in the blockchain's native token.

**Unit**: Native token (ETH, AVAX, MATIC, etc.)
**Precision Note**: May have limited decimal precision due to SUM() aggregation

**For Exact Values**: Query `tx_fee_precise` in fact_transactions

**Revenue Calculation**:
```sql
-- Daily network revenue
SELECT 
    DATE(block_timestamp_hour) AS date,
    SUM(total_fees_native) AS daily_fees_native,
    AVG(total_fees_native / NULLIF(transaction_count, 0)) AS avg_fee_per_tx
FROM <blockchain_name>.stats.ez_core_metrics_hourly
WHERE block_timestamp_hour >= CURRENT_DATE - 30
GROUP BY 1
ORDER BY 1 DESC;
```

{% enddocs %}

{% docs ez_core_metrics_hourly_total_fees_usd %}

Sum of all transaction fees paid, converted to USD.

**Precision**: Rounded to 2 decimal places
**Conversion**: Uses hourly native token price
**NULL**: When price data unavailable

**Fee Analysis**:
```sql
-- Compare fee revenue across chains
SELECT 
    blockchain,
    DATE(block_timestamp_hour) AS date,
    SUM(total_fees_usd) AS daily_revenue_usd,
    SUM(transaction_count) AS daily_transactions,
    SUM(total_fees_usd) / NULLIF(SUM(transaction_count), 0) AS avg_fee_usd
FROM <blockchain_name>.stats.ez_core_metrics_hourly
WHERE block_timestamp_hour >= CURRENT_DATE - 7
GROUP BY 1, 2
ORDER BY 3 DESC;
```

{% enddocs %}