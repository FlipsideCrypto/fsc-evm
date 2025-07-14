{% docs ez_native_transfers_table_doc %}

## What

This convenience table tracks all native asset transfers (ETH, AVAX, MATIC, etc.) extracted from transaction traces. It provides a simplified view of value movements with decimal adjustments and USD conversions, making it easy to analyze fund flows without parsing complex trace data.

## Key Use Cases

- Tracking native asset movements between wallets and contracts
- Analyzing exchange deposits and withdrawals
- Monitoring whale movements and large transfers
- Calculating wallet balances from transfer history
- Identifying internal transfers within smart contract executions

## Important Relationships

- **Join with fact_transactions**: Use `tx_hash` for transaction context
- **Join with fact_traces**: Use `tx_hash` and `trace_index` for trace details
- **Join with dim_labels**: Use addresses for entity identification
- **Complement to ez_token_transfers**: This table for native, that for tokens

## Commonly-used Fields

- `from_address`: The sender of the native asset transfer
- `to_address`: The recipient of the native asset transfer
- `amount`: Decimal-adjusted transfer amount
- `amount_usd`: USD value at time of transfer
- `origin_from_address`: Original transaction sender
- `origin_to_address`: Original transaction recipient
- `identifier`: Trace identifier (0 for external transfers)

## Sample queries

**Daily Native Asset Transfer Volume**
```sql
SELECT 
    DATE_TRUNC('day', block_timestamp) AS day,
    COUNT(*) AS transfer_count,
    COUNT(DISTINCT from_address) AS unique_senders,
    COUNT(DISTINCT to_address) AS unique_receivers,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount,
    SUM(amount_usd) AS total_usd,
    MAX(amount_usd) AS largest_transfer_usd
FROM <blockchain_name>.core.ez_native_transfers
WHERE block_timestamp >= CURRENT_DATE - 30
    AND amount > 0
GROUP BY 1
ORDER BY 1 DESC;
```

**Whale Movements (Large Transfers)**
```sql
SELECT 
    block_timestamp,
    tx_hash,
    from_address,
    to_address,
    amount,
    amount_usd,
    origin_from_address,
    origin_to_address,
    identifier
FROM <blockchain_name>.core.ez_native_transfers
WHERE amount_usd > 1000000  -- Over $1M USD
    AND block_timestamp >= CURRENT_DATE - 7
ORDER BY amount_usd DESC
LIMIT 100;
```

**Exchange Deposit/Withdrawal Patterns**
```sql
WITH exchange_addresses AS (
    SELECT DISTINCT address 
    FROM dim_labels 
    WHERE label_type = 'exchange'
)
SELECT 
    DATE_TRUNC('hour', block_timestamp) AS hour,
    CASE 
        WHEN to_address IN (SELECT address FROM exchange_addresses) THEN 'Deposit'
        WHEN from_address IN (SELECT address FROM exchange_addresses) THEN 'Withdrawal'
    END AS transfer_type,
    COUNT(*) AS transfer_count,
    SUM(amount) AS total_amount,
    SUM(amount_usd) AS total_usd
FROM <blockchain_name>.core.ez_native_transfers
WHERE block_timestamp >= CURRENT_DATE - 1
    AND (to_address IN (SELECT address FROM exchange_addresses)
         OR from_address IN (SELECT address FROM exchange_addresses))
GROUP BY 1, 2
ORDER BY 1 DESC;
```

**Internal Transfer Analysis**
```sql
-- Compare external vs internal transfers
SELECT 
    CASE 
        WHEN identifier = '0' THEN 'External Transfer'
        ELSE 'Internal Transfer'
    END AS transfer_type,
    COUNT(*) AS count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount,
    SUM(amount_usd) AS total_volume_usd
FROM <blockchain_name>.core.ez_native_transfers
WHERE block_timestamp >= CURRENT_DATE - 1
GROUP BY 1;
```

**Smart Contract Native Asset Holdings**
```sql
WITH contract_balances AS (
    SELECT 
        to_address AS address,
        SUM(amount) AS inflows
    FROM <blockchain_name>.core.ez_native_transfers
    WHERE to_address IN (SELECT address FROM dim_contracts)
    GROUP BY 1
),
outflows AS (
    SELECT 
        from_address AS address,
        SUM(amount) AS outflows
    FROM <blockchain_name>.core.ez_native_transfers
    WHERE from_address IN (SELECT address FROM dim_contracts)
    GROUP BY 1
)
SELECT 
    c.address,
    dc.name AS contract_name,
    COALESCE(c.inflows, 0) - COALESCE(o.outflows, 0) AS net_balance,
    c.inflows,
    o.outflows
FROM contract_balances c
LEFT JOIN outflows o ON c.address = o.address
LEFT JOIN <blockchain_name>.core.dim_contracts dc ON c.address = dc.address
WHERE COALESCE(c.inflows, 0) - COALESCE(o.outflows, 0) > 100  -- Over 100 native tokens
ORDER BY net_balance DESC
LIMIT 50;
```

{% enddocs %}

{% docs ez_native_transfers_amount_usd %}

USD value of the native asset transfer at the time of the transaction.

Example: 2500.50

{% enddocs %}

{% docs ez_native_transfers_amount %}

Native asset amount transferred, adjusted to standard decimal units.

Example: 1.5

{% enddocs %}

{% docs ez_native_transfers_amount_precise %}

Native asset amount transferred, decimal adjusted, returned as a string to preserve precision.

Example: '1.500000000000000000'

{% enddocs %}

{% docs ez_native_transfers_amount_precise_raw %}

Native asset amount transferred, no decimal adjustment, returned as a string to preserve precision.

Example: '1500000000000000000'

{% enddocs %}

{% docs ez_native_transfers_from_address %}

The from address for the native asset transfer. This may or may not be the same as the origin_from_address.

Example: '0x1234567890123456789012345678901234567890'

{% enddocs %}

{% docs ez_native_transfers_to_address %}

The to address for the native asset transfer. This may or may not be the same as the origin_to_address.

Example: '0xabcdefabcdefabcdefabcdefabcdefabcdefabcd'

{% enddocs %}