{% docs ez_decoded_event_logs_table_doc %}

## What

This table provides human-readable decoded event data for smart contracts where ABIs are available. It transforms raw hex-encoded logs into structured JSON with named parameters and values, making blockchain data immediately queryable without manual decoding.

## Key Use Cases

- Analyzing token transfers and approvals without manual decoding
- Tracking DEX swaps and liquidity events with named parameters
- Monitoring NFT transfers and marketplace activity
- Querying DeFi protocol interactions (lending, staking, governance)
- Building analytics on any smart contract with available ABIs

## Important Relationships

- **Join with fact_event_logs**: Use `tx_hash` and `event_index` for raw event data
- **Join with dim_contracts**: Use `contract_address` for contract metadata
- **Join with fact_transactions**: Use `tx_hash` for transaction context
- **Cross-reference ez_token_transfers**: For simplified token movement data

## Commonly-used Fields

- `contract_address`: The smart contract that emitted the event
- `event_name`: The event name from the contract ABI (e.g., Transfer, Swap)
- `decoded_log`: JSON object with decoded parameter names and values
- `contract_name`: Human-readable name of the contract
- `block_timestamp`: When the event occurred
- `tx_hash`: Transaction hash containing this event

## Sample queries

**ERC-20 Transfer Events with Proper Types**

```sql
SELECT 
    block_timestamp,
    tx_hash,
    contract_address,
    contract_name,
    event_name,
    decoded_log:from::string AS from_address,
    decoded_log:to::string AS to_address,
    decoded_log:value::numeric AS amount,
    -- Convert to decimal (assuming 18 decimals)
    decoded_log:value::numeric / POW(10, 18) AS amount_decimal
FROM <blockchain_name>.core.ez_decoded_event_logs
WHERE contract_address = LOWER('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48') -- USDC
    AND event_name = 'Transfer'
    AND block_timestamp >= CURRENT_DATE - 7
    AND decoded_log:value::numeric > 1000000000 -- Over 1000 USDC
ORDER BY block_timestamp DESC;
```

**Uniswap V3 Swap Events**

```sql
SELECT 
    block_timestamp,
    tx_hash,
    contract_address AS pool_address,
    event_name,
    decoded_log:sender::string AS sender,
    decoded_log:recipient::string AS recipient,
    decoded_log:amount0::numeric AS amount0,
    decoded_log:amount1::numeric AS amount1,
    decoded_log:sqrtPriceX96::numeric AS sqrt_price,
    decoded_log:liquidity::numeric AS liquidity,
    decoded_log:tick::integer AS tick
FROM <blockchain_name>.core.ez_decoded_event_logs
WHERE event_name = 'Swap'
    AND contract_address IN (
        SELECT address FROM dim_contracts 
        WHERE contract_name ILIKE '%Uniswap V3%'
    )
    AND block_timestamp >= CURRENT_DATE - 1
LIMIT 100;
```

**NFT Transfer Events (ERC-721)**

```sql
SELECT 
    block_timestamp,
    tx_hash,
    contract_address,
    contract_name AS collection_name,
    decoded_log:from::string AS from_address,
    decoded_log:to::string AS to_address,
    decoded_log:tokenId::string AS token_id
FROM <blockchain_name>.core.ez_decoded_event_logs
WHERE event_name = 'Transfer'
    AND decoded_log:tokenId IS NOT NULL  -- Indicates ERC-721
    AND block_timestamp >= CURRENT_DATE - 1
ORDER BY block_timestamp DESC;
```

**DeFi Protocol Events - Compound Finance**

```sql
SELECT 
    DATE_TRUNC('day', block_timestamp) AS day,
    event_name,
    COUNT(*) AS event_count,
    COUNT(DISTINCT decoded_log:minter::string) AS unique_users
FROM <blockchain_name>.core.ez_decoded_event_logs
WHERE contract_name ILIKE '%compound%'
    AND event_name IN ('Mint', 'Redeem', 'Borrow', 'RepayBorrow')
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;
```

**Complex Event Analysis - DEX Aggregator Routes**

```sql
SELECT 
    block_timestamp,
    tx_hash,
    event_name,
    decoded_log,
    ARRAY_SIZE(decoded_log:path) AS swap_hops,
    decoded_log:amountIn::numeric AS amount_in,
    decoded_log:amountOutMin::numeric AS min_amount_out
FROM <blockchain_name>.core.ez_decoded_event_logs
WHERE contract_address = LOWER('0x1111111254fb6c44bAC0beD2854e76F90643097d') -- 1inch
    AND event_name ILIKE '%swap%'
    AND block_timestamp >= CURRENT_DATE - 1;
```

{% enddocs %}

{% docs ez_decoded_event_logs_contract_name %}

Human-readable name of the smart contract emitting the event, joined from dim_contracts.

Example: 'USD Coin'

{% enddocs %}

{% docs ez_decoded_event_logs_event_name %}

The event name as defined in the contract's ABI.

Example: 'Transfer'

{% enddocs %}

{% docs ez_decoded_event_logs_decoded_log %}

Flattened JSON object containing decoded event parameters with their values.

Example: '{"from": "0x123...", "to": "0x456...", "value": "1000000000000000000"}'

{% enddocs %}

{% docs ez_decoded_event_logs_full_decoded_log %}

Complete decoded event data including parameter names, values, types, and metadata.

Example: '{"event_name": "Transfer", "parameters": [{"name": "from", "type": "address", "value": "0x123...", "indexed": true}]}'

{% enddocs %}