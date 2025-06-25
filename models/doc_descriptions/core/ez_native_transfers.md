{% docs ez_native_transfers_table_doc %}

## Table: ez_native_transfers

This convenience table tracks all native asset transfers (ETH, AVAX, MATIC, etc.) extracted from transaction traces. It provides a simplified view of value movements with decimal adjustments and USD conversions, making it easy to analyze fund flows without parsing complex trace data.

### Key Features:
- **Complete Coverage**: All native token movements from both external transactions and internal transfers
- **Decimal Adjusted**: Amounts converted from Wei to standard units (e.g., ETH)
- **USD Values**: Historical USD prices at time of transfer
- **Simplified Structure**: Flattened view of trace data for easy querying
- **Origin Tracking**: Links to original transaction sender/receiver

### Data Sources:
- **External Transfers**: From fact_transactions where value > 0
- **Internal Transfers**: From fact_traces where type = 'CALL' and value > 0
- **Failed Transfers**: Excluded - only successful value movements
- **Price Data**: Historical native asset prices for USD conversion

### Native Token Mapping:
| Blockchain | Native Token | Decimals |
|------------|--------------|----------|
| ETHEREUM   | ETH          | 18       |
| BINANCE    | BNB          | 18       |
| POLYGON    | POL          | 18       |
| AVALANCHE  | AVAX         | 18       |
| ARBITRUM   | ETH          | 18       |
| OPTIMISM   | ETH          | 18       |
| GNOSIS     | xDAI         | 18       |
| BASE       | ETH          | 18       |
| MANTLE     | MNT          | 18       |
| SCROLL     | ETH          | 18       |
| BOB        | ETH          | 18       |
| BOBA       | ETH          | 18       |
| CORE       | ETH          | 18       |
| INK        | ETH          | 18       |
| RONIN      | ETH          | 18       |
| SCROLL     | ETH          | 18       |
| SWELL      | ETH          | 18       |

### Important Relationships:
- **Join with fact_transactions**: Use `tx_hash` for transaction context
- **Join with fact_traces**: Use `tx_hash` and `trace_index` for trace details
- **Join with dim_labels**: Use addresses for entity identification
- **Complement to ez_token_transfers**: This table for native, that for tokens

### Sample Queries:

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

### Critical Usage Notes:
- **No Token Transfers**: This table excludes ERC-20, ERC-721, ERC-1155 transfers
- **Success Only**: Failed transfers are excluded
- **Price Timing**: USD values calculated at block timestamp
- **Trace Coverage**: Includes all successful traces with value > 0

### Performance Tips:
- Always include block_timestamp in WHERE clause
- Use amount_usd for cross-chain value comparisons
- Filter by origin addresses for transaction-level analysis
- Consider indexing on from_address, to_address for wallet analysis

{% enddocs %}

{% docs evm_amount %}

Native asset amount transferred, adjusted to standard decimal units.

**Format**: DECIMAL(38,0) - preserves precision
**Conversion**: Raw Wei value / 10^18
**Examples**:
- 1.5 = 1.5 ETH (or native asset)
- 0.001 = 0.001 ETH
- 1000 = 1000 ETH

**Key Points**:
- Always positive (transfers have direction via from/to)
- Decimal adjusted (not in Wei)
- Use for calculations and aggregations

**Query Examples**:
```sql
-- Distribution of transfer sizes
SELECT 
    CASE 
        WHEN amount < 0.1 THEN '< 0.1'
        WHEN amount < 1 THEN '0.1 - 1'
        WHEN amount < 10 THEN '1 - 10'
        WHEN amount < 100 THEN '10 - 100'
        ELSE '> 100'
    END AS size_bucket,
    COUNT(*) AS transfer_count,
    SUM(amount) AS total_amount
FROM <blockchain_name>.core.ez_native_transfers
WHERE block_timestamp >= CURRENT_DATE - 7
GROUP BY 1
ORDER BY MIN(amount);
```

{% enddocs %}

{% docs ez_native_transfers_amount_usd %}

USD value of the native asset transfer at the time of the transaction.

**Calculation**: amount * native_asset_price_usd
**Price Source**: Historical price feeds at block timestamp
**Precision**: 2 decimal places typical

**NULL When**:
- Price data unavailable for timestamp
- Very early blockchain history
- Price feed issues

**Use Cases**:
- Cross-chain value comparisons
- Historical portfolio analysis
- Large transfer monitoring
- Volume aggregations in USD

**Query Example**:
```sql
-- Daily USD volume by transfer size
SELECT 
    DATE_TRUNC('day', block_timestamp) AS day,
    CASE 
        WHEN amount_usd < 100 THEN '< $100'
        WHEN amount_usd < 1000 THEN '$100 - $1K'
        WHEN amount_usd < 10000 THEN '$1K - $10K'
        WHEN amount_usd < 100000 THEN '$10K - $100K'
        ELSE '> $100K'
    END AS size_category,
    COUNT(*) AS transfers,
    SUM(amount_usd) AS total_usd
FROM <blockchain_name>.core.ez_native_transfers
WHERE block_timestamp >= CURRENT_DATE - 30
    AND amount_usd IS NOT NULL
GROUP BY 1, 2
ORDER BY 1 DESC, MIN(amount_usd);
```

{% enddocs %}

{% docs ez_native_transfers_amount %}

Native asset amount transferred, adjusted to standard decimal units.

**Format**: DECIMAL(38,0) - preserves precision
**Conversion**: Raw Wei value / 10^18
**Examples**:
- 1.5 = 1.5 ETH (or native asset)
- 0.001 = 0.001 ETH
- 1000 = 1000 ETH

{% enddocs %}

{% docs ez_native_transfers_amount_precise %}

Native asset amount transferred, decimal adjusted, returned as a string to preserve precision.

**Format**: VARCHAR(38) - preserves precision
**Conversion**: Raw Wei value / 10^18
**Examples**:
- 1.5 = 1.5 ETH (or native asset)
- 0.001 = 0.001 ETH
- 1000 = 1000 ETH

{% enddocs %}

{% docs ez_native_transfers_amount_precise_raw %}

Native asset amount transferred, no decimal adjustment, returned as a string to preserve precision.

**Format**: VARCHAR(38) - preserves precision
**Conversion**: Raw Wei value / 10^18
**Examples**:
- 1.5 = 1.5 ETH (or native asset)
- 0.001 = 0.001 ETH
- 1000 = 1000 ETH

{% enddocs %}

{% docs ez_native_transfers_from_address %}

The from address for the native asset transfer. This may or may not be the same as the origin_from_address.

**Format**: VARCHAR(42) - 40 character address
**Examples**:
- 0x1234567890123456789012345678901234567890
- 0x1234567890123456789012345678901234567890

{% enddocs %}

{% docs ez_native_transfers_to_address %}

The to address for the native asset transfer. This may or may not be the same as the origin_to_address.

**Format**: VARCHAR(42) - 40 character address
**Examples**:
- 0x1234567890123456789012345678901234567890
- 0x1234567890123456789012345678901234567890

{% enddocs %}