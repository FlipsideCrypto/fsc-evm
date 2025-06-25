{% docs ez_decoded_event_logs_table_doc %}

## Table: ez_decoded_event_logs

This table provides human-readable decoded event data for smart contracts where ABIs are available. It transforms raw hex-encoded logs into structured JSON with named parameters and values, making blockchain data immediately queryable without manual decoding.

### Key Features:
- **Automated Decoding**: Raw event data automatically parsed using contract ABIs
- **JSON Structure**: Easy dot-notation access to event parameters
- **Type Safety**: Proper data types for all decoded values
- **Coverage**: Includes major protocols, tokens, and verified contracts

### ABI Coverage:
- **Automatic**: Verified contracts on block explorers
- **Manual Submission**: Request decoding at [ABI Requestor](https://science.flipsidecrypto.xyz/abi-requestor/)
- **Updates**: New ABIs added continuously
- **Popular Contracts**: All major DeFi protocols, NFT collections, and tokens

### Important Relationships:
- **Join with fact_event_logs**: Use `tx_hash` and `event_index` for raw event data
- **Join with dim_contracts**: Use `contract_address` for contract metadata
- **Join with fact_transactions**: Use `tx_hash` for transaction context
- **Cross-reference ez_token_transfers**: For simplified token movement data

### Query Patterns:

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

### Common Event Patterns:

| Protocol Type | Common Events | Key Parameters |
|--------------|---------------|----------------|
| ERC-20 Tokens | Transfer, Approval | from, to, value |
| ERC-721 NFTs | Transfer, Approval, ApprovalForAll | from, to, tokenId |
| DEX AMMs | Swap, Mint, Burn, Sync | amount0, amount1, to |
| Lending | Deposit, Withdraw, Borrow, Repay | user, amount, asset |
| Staking | Staked, Withdrawn, RewardPaid | user, amount, reward |

### Data Type Casting:

```sql
-- String addresses
decoded_log:address::string

-- Numeric amounts (preserves precision)
decoded_log:amount::numeric

-- Integer values
decoded_log:tokenId::integer

-- Boolean flags
decoded_log:approved::boolean

-- Array access
decoded_log:path[0]::string

-- Nested objects
decoded_log:data:innerField::string
```

### Performance Tips:
- Filter by contract_address when possible
- Use event_name to target specific events
- Add block_timestamp constraints for time ranges
- Consider using fact_event_logs for simpler queries on topic_0

### Coverage Verification:

```sql
-- Check if a contract has decoded events
SELECT 
    contract_address,
    contract_name,
    COUNT(DISTINCT event_name) AS decoded_event_types,
    MIN(block_timestamp) AS first_decoded,
    MAX(block_timestamp) AS last_decoded,
    COUNT(*) AS total_events
FROM ez_decoded_event_logs
WHERE contract_address = LOWER('0x[YOUR_CONTRACT_ADDRESS]')
GROUP BY 1, 2;
```

{% enddocs %}

{% docs ez_decoded_event_logs_contract_name %}

Human-readable name of the smart contract emitting the event, joined from dim_contracts.

**Sources**:
- Contract name() function
- Verified contract metadata
- Token lists and registries
- Manual labeling for popular contracts

**Examples**:
- "USD Coin" (USDC)
- "Uniswap V3: USDC-ETH 0.05%"
- "OpenSea: Seaport 1.5"

**NULL When**: Contract not verified or no name available

{% enddocs %}

{% docs ez_decoded_event_logs_event_name %}

The event name as defined in the contract's ABI.

**Format**: PascalCase event identifier
**Examples**:
- `Transfer` - Token transfers
- `Swap` - DEX trades  
- `OwnershipTransferred` - Admin changes
- `Approval` - Token approvals

**Usage Pattern**:

```sql
-- Find all event types for a contract
SELECT DISTINCT event_name, COUNT(*) as occurrences
FROM ez_decoded_event_logs
WHERE contract_address = LOWER('0x...')
GROUP BY 1
ORDER BY 2 DESC;
```

{% enddocs %}

{% docs ez_decoded_event_logs_decoded_log %}

Flattened JSON object containing decoded event parameters with their values.

**Structure**: `{parameter_name: parameter_value, ...}`
**Access Pattern**: `decoded_log:<parameter>::<type>`

**Examples**:

```json
// ERC-20 Transfer
{
  "from": "0x123...",
  "to": "0x456...",
  "value": "1000000000000000000"
}

// Uniswap V2 Swap
{
  "sender": "0x789...",
  "amount0In": "0",
  "amount1In": "1000000000",
  "amount0Out": "2000000000000000000",
  "amount1Out": "0",
  "to": "0xabc..."
}
```

**Query Examples**:

```sql
-- Access specific fields
SELECT 
    decoded_log:from::string AS sender,
    decoded_log:to::string AS recipient,
    decoded_log:value::numeric / 1e18 AS amount_eth
FROM <blockchain_name>.core.ez_decoded_event_logs
WHERE event_name = 'Transfer';

-- Filter by decoded values
WHERE decoded_log:value::numeric > 1000000000000000000  -- > 1 ETH
```

{% enddocs %}

{% docs ez_decoded_event_logs_full_decoded_log %}

Complete decoded event data including parameter names, values, types, and metadata.

**Additional Fields**:
- Parameter data types (uint256, address, etc.)
- Parameter indexing status
- Raw vs decoded values
- Decoding metadata

**Structure Example**:

```json
{
  "event_name": "Transfer",
  "parameters": [
    {
      "name": "from",
      "type": "address",
      "value": "0x123...",
      "indexed": true
    },
    {
      "name": "to",
      "type": "address", 
      "value": "0x456...",
      "indexed": true
    },
    {
      "name": "value",
      "type": "uint256",
      "value": "1000000000000000000",
      "indexed": false
    }
  ]
}
```

**Usage**:

```sql
-- Lateral flatten for detailed analysis
SELECT 
    f.block_timestamp,
    f.tx_hash,
    f.event_name,
    p.value:name::string AS param_name,
    p.value:type::string AS param_type,
    p.value:value::string AS param_value,
    p.value:indexed::boolean AS is_indexed
FROM <blockchain_name>.core.ez_decoded_event_logs f,
LATERAL FLATTEN(input => f.full_decoded_log:parameters) p
WHERE f.contract_address = LOWER('0x...')
LIMIT 100;
```

{% enddocs %}