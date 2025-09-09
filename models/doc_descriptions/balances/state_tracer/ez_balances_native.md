{% docs ez_balances_native_table_doc %}

## What

This table tracks native asset balance changes (ETH, AVAX, BNB, etc.) at the transaction level by capturing pre- and post-transaction states. It uses state tracer data to show exactly how each address's native asset balance changed during transaction execution, including decimal adjustments and USD valuations, where available, for comprehensive balance analysis. This data set includes both successful and failed transactions, as state may change regardless.

## Key Use Cases

- Tracking native asset balance changes at transaction granularity
- Analyzing balance impacts of smart contract interactions
- Monitoring large balance changes and whale activity
- Calculating precise balance evolution over time
- Identifying addresses with significant native asset holdings
- Debugging transaction effects on account balances

## Important Relationships

- **Join with fact_transactions**: Use `tx_hash` for transaction context
- **Join with fact_blocks**: Use `block_number` for block metadata
- **Join with dim_labels**: Use `address` for entity identification
- **Join with ez_native_transfers**: Compare balance changes to transfer amounts
- **Join with ez_prices_hourly**: USD valuations already included but can be refreshed

## Commonly-used Fields

- `address`: The account whose balance changed
- `pre_balance`: Native asset balance before the transaction
- `post_balance`: Native asset balance after the transaction
- `net_balance`: The change in balance (post - pre)
- `pre_balance_usd` / `post_balance_usd`: USD values at time of transaction
- `tx_hash`: Transaction that caused the balance change
- `block_timestamp`: When the balance change occurred

## Sample queries

**Daily Native Asset Balance Changes**
```sql
SELECT 
    DATE_TRUNC('day', block_timestamp) AS day,
    COUNT(*) AS balance_changes,
    COUNT(DISTINCT address) AS unique_addresses,
    SUM(ABS(net_balance)) AS total_balance_moved,
    SUM(CASE WHEN net_balance > 0 THEN net_balance ELSE 0 END) AS total_increases,
    SUM(CASE WHEN net_balance < 0 THEN ABS(net_balance) ELSE 0 END) AS total_decreases
FROM <blockchain_name>.balances.ez_balances_native
WHERE block_timestamp >= CURRENT_DATE - 30
    AND net_balance != 0
GROUP BY 1
ORDER BY 1 DESC;
```

**Address Balance Evolution**
```sql
-- Track how a specific address's balance changed over time
SELECT 
    block_timestamp,
    tx_hash,
    pre_balance,
    post_balance,
    net_balance,
    pre_balance_usd,
    post_balance_usd,
    SUM(net_balance) OVER (
        PARTITION BY address 
        ORDER BY block_number, tx_position 
        ROWS UNBOUNDED PRECEDING
    ) AS running_balance_change
FROM <blockchain_name>.balances.ez_balances_native
WHERE address = LOWER('0x1234567890123456789012345678901234567890')
    AND block_timestamp >= CURRENT_DATE - 30
ORDER BY block_timestamp DESC;
```

**Smart Contract Balance Impact Analysis**
```sql
-- Analyze how smart contract interactions affect user balances
WITH contract_interactions AS (
    SELECT 
        b.address,
        b.tx_hash,
        b.net_balance,
        b.block_timestamp,
        t.to_address AS contract_interacted
    FROM <blockchain_name>.balances.ez_balances_native b
    JOIN <blockchain_name>.core.fact_transactions t USING (tx_hash)
    WHERE b.net_balance != 0
        AND t.to_address IN (SELECT address FROM dim_contracts)
        AND b.block_timestamp >= CURRENT_DATE - 7
)
SELECT 
    contract_interacted,
    dc.name AS contract_name,
    COUNT(*) AS balance_changes,
    COUNT(DISTINCT address) AS unique_users,
    SUM(CASE WHEN net_balance > 0 THEN net_balance ELSE 0 END) AS total_gains,
    SUM(CASE WHEN net_balance < 0 THEN ABS(net_balance) ELSE 0 END) AS total_losses,
    AVG(ABS(net_balance)) AS avg_balance_change
FROM contract_interactions c
LEFT JOIN <blockchain_name>.core.dim_contracts dc ON c.contract_interacted = dc.address
GROUP BY 1, 2
HAVING COUNT(*) > 10
ORDER BY total_gains + total_losses DESC
LIMIT 50;
```

**Balance Change Distribution**
```sql
-- Analyze the distribution of balance changes by magnitude
SELECT 
    CASE 
        WHEN ABS(net_balance) < 0.01 THEN 'Dust (<0.01)'
        WHEN ABS(net_balance) < 0.1 THEN 'Small (0.01-0.1)'
        WHEN ABS(net_balance) < 1 THEN 'Medium (0.1-1)'
        WHEN ABS(net_balance) < 10 THEN 'Large (1-10)'
        WHEN ABS(net_balance) < 100 THEN 'Very Large (10-100)'
        ELSE 'Whale (100+)'
    END AS balance_change_category,
    COUNT(*) AS change_count,
    COUNT(DISTINCT address) AS unique_addresses,
    SUM(ABS(net_balance)) AS total_volume,
    AVG(ABS(net_balance)) AS avg_magnitude
FROM <blockchain_name>.balances.ez_balances_native
WHERE block_timestamp >= CURRENT_DATE - 1
    AND net_balance != 0
GROUP BY 1
ORDER BY 
    CASE balance_change_category
        WHEN 'Dust (<0.01)' THEN 1
        WHEN 'Small (0.01-0.1)' THEN 2
        WHEN 'Medium (0.1-1)' THEN 3
        WHEN 'Large (1-10)' THEN 4
        WHEN 'Very Large (10-100)' THEN 5
        WHEN 'Whale (100+)' THEN 6
    END;
```

{% enddocs %}

{% docs ez_balances_native_address %}

The address whose native asset balance changed in this transaction.

Example: '0x1234567890123456789012345678901234567890'

{% enddocs %}

{% docs ez_balances_native_pre_balance %}

Native asset balance before the transaction execution, decimal adjusted to standard units.

Example: 15.75

{% enddocs %}

{% docs ez_balances_native_post_balance %}

Native asset balance after the transaction execution, decimal adjusted to standard units.

Example: 12.25

{% enddocs %}

{% docs ez_balances_native_net_balance %}

The change in native asset balance (post_balance - pre_balance).

Example: -3.5

{% enddocs %}

{% docs ez_balances_native_pre_balance_usd %}

USD value of the pre-transaction balance at the time of the transaction.

Example: 39375.00

{% enddocs %}

{% docs ez_balances_native_post_balance_usd %}

USD value of the post-transaction balance at the time of the transaction.

Example: 30625.00

{% enddocs %}

{% docs ez_balances_native_pre_balance_precise %}

Native asset balance before transaction, decimal adjusted, returned as a string to preserve precision.

Example: '15.750000000000000000'

{% enddocs %}

{% docs ez_balances_native_post_balance_precise %}

Native asset balance after transaction, decimal adjusted, returned as a string to preserve precision.

Example: '12.250000000000000000'

{% enddocs %}

{% docs ez_balances_native_pre_balance_raw %}

Native asset balance before transaction in smallest unit (Wei), no decimal adjustment.

Example: 15750000000000000000

{% enddocs %}

{% docs ez_balances_native_post_balance_raw %}

Native asset balance after transaction in smallest unit (Wei), no decimal adjustment.

Example: 12250000000000000000

{% enddocs %}

{% docs ez_balances_native_net_balance_raw %}

The change in native asset balance in smallest unit (Wei).

Example: -3500000000000000000

{% enddocs %}

{% docs ez_balances_native_pre_balance_hex %}

Hexadecimal representation of the pre-transaction balance as returned by the blockchain RPC.

Example: '0xda475abf0000000'

{% enddocs %}

{% docs ez_balances_native_post_balance_hex %}

Hexadecimal representation of the post-transaction balance as returned by the blockchain RPC.

Example: '0xaa87bee5380000'

{% enddocs %}

{% docs ez_balances_native_pre_nonce %}

Account nonce value before the transaction execution.

Example: 42

{% enddocs %}

{% docs ez_balances_native_post_nonce %}

Account nonce value after the transaction execution. May be null.

Example: 43

{% enddocs %}

{% docs ez_balances_native_decimals %}

Number of decimal places for the native asset. Typically 18 for native assets.

Example: 18

{% enddocs %}