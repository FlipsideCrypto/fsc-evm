{% docs ez_token_transfers_table_doc %}

## What

This convenience table provides a comprehensive view of all ERC-20 token transfers with enriched metadata including decimal adjustments, USD values, and token information. It simplifies token flow analysis by joining transfer events with contract details and price data.

## Key Use Cases

- Tracking token movements and holder activity
- Analyzing stablecoin flows and volumes
- Monitoring DEX token inflows and outflows
- Detecting new token launches and adoption
- Calculating wallet token balances from transfer history

## Important Relationships

- **Join with fact_event_logs**: Use `tx_hash` and `event_index` for raw event details
- **Join with fact_transactions**: Use `tx_hash` for transaction context
- **Join with dim_contracts**: Use `contract_address` for token metadata
- **Complement to ez_native_transfers**: Complete picture of value flows

## Commonly-used Fields

- `contract_address`: The token contract address (NOT the recipient)
- `from_address`: Token sender address
- `to_address`: Token recipient address
- `amount`: Decimal-adjusted transfer amount
- `amount_usd`: USD value at time of transfer
- `symbol`: Token symbol (e.g., USDC, DAI)
- `raw_amount`: Original amount without decimal adjustment

## Sample queries

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

{% enddocs %}

{% docs ez_token_transfers_from_address %}

The from address for the token transfer. This may or may not be the same as the origin_from_address.

Example: '0x1234567890123456789012345678901234567890'

{% enddocs %}

{% docs ez_token_transfers_to_address %}

The to address for the token transfer. This may or may not be the same as the origin_to_address.

Example: '0xabcdefabcdefabcdefabcdefabcdefabcdefabcd'

{% enddocs %}

{% docs ez_token_transfers_contract_address %}

The contract address for the token transfer.

Example: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'

{% enddocs %}

{% docs ez_token_transfers_token_standard %}

The token standard for the transfer, in this case always erc20.

Example: 'erc20'

{% enddocs %}

{% docs ez_token_transfers_token_is_verified %}

Boolean flag indicating if the token is verified by the Flipside team.

Example: true

{% enddocs %}

{% docs ez_token_transfers_amount %}

Decimal-adjusted token amount for human-readable values.

Example: 1000.50

{% enddocs %}

{% docs ez_token_transfers_amount_precise %}

String representation of decimal-adjusted amount preserving full precision.

Example: '1000.500000'

{% enddocs %}

{% docs ez_token_transfers_amount_usd %}

USD value of the token transfer at transaction time.

Example: 1000.50

{% enddocs %}

{% docs ez_token_transfers_raw_amount %}

Original token amount without decimal adjustment.

Example: 1000500000

{% enddocs %}

{% docs ez_token_transfers_raw_amount_precise %}

String representation of raw amount for precision preservation.

Example: '1000500000'

{% enddocs %}