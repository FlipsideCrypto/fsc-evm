{% docs ez_token_transfers_table_doc %}

## Table: ez_token_transfers

This convenience table provides a comprehensive view of all ERC-20 token transfers with enriched metadata including decimal adjustments, USD values, and token information. It simplifies token flow analysis by joining transfer events with contract details and price data, eliminating the need for complex manual joins.

### Key Features:
- **ERC-20 Focus**: Specifically tracks fungible token transfers (not NFTs or native assets)
- **Decimal Adjusted**: Automatic conversion from raw amounts to human-readable values
- **USD Valuations**: Historical USD values at time of transfer
- **Token Metadata**: Includes symbol, name, decimals from dim_contracts
- **Price Integration**: Hourly token prices where available

### Event Coverage:
- **Transfer Event**: `0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef`
- **Standard**: ERC-20 compliant tokens only
- **Excludes**: Native assets (use ez_native_transfers), NFTs (use nft.ez_nft_transfers)

### Important Relationships:
- **Join with fact_event_logs**: Use `tx_hash` and `event_index` for raw event details
- **Join with fact_transactions**: Use `tx_hash` for transaction context
- **Join with dim_contracts**: Use `contract_address` for token metadata
- **Complement to ez_native_transfers**: Complete picture of value flows

### Sample Queries:

**Top Token Transfers by USD Value**
```sql
SELECT 
    block_timestamp,
    tx_hash,
    symbol,
    from_address,
    to_address,
    amount,
    amount_usd,
    token_price,
    contract_address
FROM <blockchain_name>.core.ez_token_transfers
WHERE block_timestamp >= CURRENT_DATE - 7
    AND amount_usd > 100000  -- Over $100k
    AND has_decimal = TRUE
    AND has_price = TRUE
ORDER BY amount_usd DESC
LIMIT 100;
```

**Daily Stablecoin Volume Analysis**
```sql
SELECT 
    DATE_TRUNC('day', block_timestamp) AS day,
    symbol,
    COUNT(*) AS transfer_count,
    COUNT(DISTINCT from_address) AS unique_senders,
    SUM(amount) AS total_amount,
    SUM(amount_usd) AS total_usd,
    AVG(amount_usd) AS avg_transfer_usd
FROM <blockchain_name>.core.ez_token_transfers
WHERE symbol IN ('USDC', 'USDT', 'DAI', 'BUSD')
    AND block_timestamp >= CURRENT_DATE - 30
    AND has_decimal = TRUE
GROUP BY 1, 2
ORDER BY 1 DESC, 6 DESC;
```

**DEX Token Flow Analysis**
```sql
WITH dex_addresses AS (
    SELECT address 
    FROM <blockchain_name>.core.dim_labels 
    WHERE label_type = 'dex' 
    AND label_subtype IN ('pool', 'router')
)
SELECT 
    DATE_TRUNC('hour', block_timestamp) AS hour,
    symbol,
    CASE 
        WHEN from_address IN (SELECT address FROM dex_addresses) THEN 'DEX Outflow'
        WHEN to_address IN (SELECT address FROM dex_addresses) THEN 'DEX Inflow'
    END AS flow_type,
    COUNT(*) AS transfers,
    SUM(amount) AS total_amount,
    SUM(amount_usd) AS total_usd
FROM <blockchain_name>.core.ez_token_transfers
WHERE block_timestamp >= CURRENT_DATE - 1
    AND (from_address IN (SELECT address FROM dex_addresses)
         OR to_address IN (SELECT address FROM dex_addresses))
    AND symbol IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 6 DESC;
```

**Token Holder Activity**
```sql
-- Most active token senders
SELECT 
    from_address,
    COUNT(DISTINCT contract_address) AS tokens_sent,
    COUNT(DISTINCT DATE(block_timestamp)) AS active_days,
    COUNT(*) AS total_transfers,
    SUM(amount_usd) AS total_usd_sent
FROM <blockchain_name>.core.ez_token_transfers
WHERE block_timestamp >= CURRENT_DATE - 30
    AND has_price = TRUE
    AND amount_usd > 10  -- Filter dust
GROUP BY 1
HAVING COUNT(*) > 10
ORDER BY total_usd_sent DESC
LIMIT 100;
```

**New Token Detection**
```sql
WITH first_transfers AS (
    SELECT 
        contract_address,
        symbol,
        MIN(block_timestamp) AS first_transfer,
        COUNT(*) AS transfer_count,
        COUNT(DISTINCT from_address) AS unique_senders,
        COUNT(DISTINCT to_address) AS unique_receivers
    FROM <blockchain_name>.core.ez_token_transfers
    WHERE block_timestamp >= CURRENT_DATE - 7
    GROUP BY 1, 2
    HAVING MIN(block_timestamp) >= CURRENT_DATE - 1
)
SELECT 
    ft.*,
    dc.name AS token_name,
    dc.decimals
FROM first_transfers ft
LEFT JOIN <blockchain_name>.core.dim_contracts dc ON ft.contract_address = dc.address
ORDER BY transfer_count DESC;
```

### Performance Optimization:
- Filter by block_timestamp first
- Use has_decimal and has_price flags
- Consider contract_address for specific tokens
- Symbol is not a safe filter as any token can have the same symbol, use contract_address for specific tokens

{% enddocs %}

{% docs ez_token_transfers_from_address %}

The from address for the token transfer. This may or may not be the same as the origin_from_address.

**Special Values**:
- `0x0000...0000`: Minting event (tokens created)
- Contract addresses: Protocol interactions
- EOAs: User transfers

**Format**: VARCHAR(42) - 40 character address
**Examples**:
- 0x1234567890123456789012345678901234567890
- 0x1234567890123456789012345678901234567890

{% enddocs %}

{% docs ez_token_transfers_to_address %}

The to address for the token transfer. This may or may not be the same as the origin_to_address.

Address receiving the tokens in this transfer.

**Special Values**:
- `0x0000...0000`: Burning event (tokens destroyed)
- Contract addresses: DeFi deposits, DEX swaps
- EOAs: User receipts

**Burn Detection**:
```sql
-- Token burn events
SELECT 
    DATE(block_timestamp) AS burn_date,
    symbol,
    COUNT(*) AS burn_count,
    SUM(amount) AS total_burned,
    SUM(amount_usd) AS usd_burned
FROM <blockchain_name>.core.ez_token_transfers
WHERE to_address = '0x0000000000000000000000000000000000000000'
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY 1, 2
ORDER BY 5 DESC;
```

{% enddocs %}

{% docs ez_token_transfers_contract_address %}

The contract address for the token transfer.

**Format**: VARCHAR(42) - 40 character address
**Examples**:
- 0x1234567890123456789012345678901234567890
- 0x1234567890123456789012345678901234567890
**Note**: This is NOT the recipient - it's the token. It is also the best way to identify the token being transferred.

{% enddocs %}

{% docs ez_token_transfers_token_standard %}

The token standard for the transfer, in this case always erc20.

{% enddocs %}

{% docs ez_token_transfers_token_is_verified %}

Boolean flag indicating if the token is verified by the Flipside team.

**Format**: BOOLEAN

{% enddocs %}

{% docs ez_token_transfers_amount %}

Decimal-adjusted token amount for human-readable values.

**Calculation**: raw_amount / 10^decimals
**NULL When**: 
- Token decimals unknown (has_decimal = FALSE)
- Zero value transfers

**Examples**:
- Raw: 1000000, Decimals: 6 → Amount: 1.0 (USDC)
- Raw: 1000000000000000000, Decimals: 18 → Amount: 1.0 (DAI)

{% enddocs %}

{% docs ez_token_transfers_amount_precise %}

String representation of decimal-adjusted amount preserving full precision.

**Format**: VARCHAR to prevent floating point errors
**Use Cases**:
- Exact balance calculations
- Audit reconciliation
- Large value transfers

**Conversion Example**:
```sql
-- Safe aggregation with precise amounts
SELECT 
    symbol,
    SUM(CAST(amount_precise AS NUMERIC(38,18))) AS total_precise
FROM ez_token_transfers
WHERE block_timestamp >= CURRENT_DATE - 1
GROUP BY 1;
```

{% enddocs %}

{% docs ez_token_transfers_amount_usd %}

USD value of the token transfer at transaction time.

**Calculation**: amount * token_price
**NULL When**:
- No price data (has_price = FALSE)
- No decimal data (has_decimal = FALSE)
- Token not tracked by price feeds

**Price Timing**: Hourly price at block_timestamp
**Coverage**: Major tokens, varies by chain

**Value Analysis**:
```sql
-- Distribution of transfer values
SELECT 
    CASE 
        WHEN amount_usd < 10 THEN '< $10'
        WHEN amount_usd < 100 THEN '$10-100'
        WHEN amount_usd < 1000 THEN '$100-1K'
        WHEN amount_usd < 10000 THEN '$1K-10K'
        ELSE '> $10K'
    END AS value_bucket,
    COUNT(*) AS transfers,
    SUM(amount_usd) AS total_usd
FROM <blockchain_name>.core.ez_token_transfers
WHERE block_timestamp >= CURRENT_DATE - 7
GROUP BY 1
ORDER BY MIN(amount_usd);
```

{% enddocs %}

{% docs ez_token_transfers_raw_amount %}

Original token amount without decimal adjustment.

**Format**: Raw blockchain value
**Use Cases**:
- Verification with blockchain explorers
- Custom decimal handling
- Reconciliation checks

**Relationship**: amount = raw_amount / 10^decimals

{% enddocs %}

{% docs ez_token_transfers_raw_amount_precise %}

String representation of raw amount for precision preservation.

**Format**: VARCHAR for exact values
**Purpose**: Prevent numeric overflow or precision loss

**Usage Example**:
```sql
-- Compare raw vs adjusted
SELECT 
    symbol,
    raw_amount_precise,
    amount_precise,
    decimals
FROM <blockchain_name>.core.ez_token_transfers
JOIN <blockchain_name>.core.dim_contracts USING (contract_address)
WHERE symbol = 'USDC' and token_is_verified
LIMIT 5;
```
{% enddocs %}