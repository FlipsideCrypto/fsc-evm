{% docs ez_core_metrics_hourly_table_doc %}

## What

This aggregated table provides hourly blockchain metrics for high-level analysis and monitoring. It pre-calculates key statistics from fact_transactions to enable fast querying of network activity, gas usage patterns, and user behavior trends without scanning large transaction tables.

## Key Use Cases

- Network health dashboards and monitoring
- Gas fee trend analysis and volatility tracking
- User adoption metrics and activity patterns
- Blockchain comparison studies across chains
- Activity anomaly detection and congestion analysis
- Performance optimization and capacity planning
- Weekly/monthly growth analysis

## Important Relationships

- **Derived from fact_transactions**: All metrics aggregated from base transaction data
- **Join with ez_prices_hourly**: For native token price correlation
- **Compare across chains**: Standardized metrics enable cross-chain analysis

## Commonly-used Fields

- `block_timestamp_hour`: Hour boundary for aggregated metrics
- `transaction_count`: Total transactions in the hour
- `transaction_count_success` / `transaction_count_failed`: Success/failure counts
- `unique_from_count`: Distinct active addresses
- `total_fees_native` / `total_fees_usd`: Fee totals in native and USD
- `block_count`: Number of blocks produced

## Sample Queries

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

{% enddocs %}

{% docs ez_core_metrics_hourly_hour %}

The hour boundary (UTC) for which metrics are aggregated. Timestamp truncated to hour boundary.

Example: '2024-01-15 14:00:00.000'

{% enddocs %}

{% docs ez_core_metrics_hourly_block_number_min %}

The lowest block number produced within this hour. Used to identify first block and calculate block ranges.

Example: 18750000

{% enddocs %}

{% docs ez_core_metrics_hourly_block_number_max %}

The highest block number produced within this hour. Used to identify last block and monitor chain tip progression.

Example: 18750299

{% enddocs %}

{% docs ez_core_metrics_hourly_block_count %}

Total number of blocks produced in the hour. Calculated as block_number_max - block_number_min + 1.

Example: 300

{% enddocs %}

{% docs ez_core_metrics_hourly_transaction_count %}

Total number of transactions included in blocks during this hour. Includes both successful and failed transactions.

Example: 125000

{% enddocs %}

{% docs ez_core_metrics_hourly_count_success %}

Number of transactions that executed successfully in the hour. Used to calculate network reliability.

Example: 118750

{% enddocs %}

{% docs ez_core_metrics_hourly_transaction_count_failed %}

Number of transactions that failed or reverted in the hour. Common causes include insufficient gas or contract reverts.

Example: 6250

{% enddocs %}

{% docs ez_core_metrics_hourly_unique_from_count %}

Count of distinct addresses that initiated transactions in the hour. Represents active users, not cumulative.

Example: 45000

{% enddocs %}

{% docs ez_core_metrics_hourly_unique_to_count %}

Count of distinct addresses that received transactions in the hour. Includes EOA recipients and contract addresses.

Example: 52000

{% enddocs %}

{% docs ez_core_metrics_hourly_total_fees_native %}

Sum of all transaction fees paid in the blockchain's native token. May have limited precision due to aggregation.

Example: 125.75

{% enddocs %}

{% docs ez_core_metrics_hourly_total_fees_usd %}

Sum of all transaction fees paid, converted to USD using hourly native token price. Rounded to 2 decimal places.

Example: 425000.50

{% enddocs %}