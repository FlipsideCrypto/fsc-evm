{% docs fact_event_logs_table_doc %}

## Table: fact_event_logs

This table contains raw event logs emitted by smart contracts during transaction execution. Each row represents a single event with its topics and data. Events are the primary mechanism for smart contracts to communicate state changes and must be explicitly emitted in contract code.

### Key Concepts:
- **Events**: Structured logs emitted by smart contracts using `emit` statements
- **Topics**: Indexed parameters for efficient filtering (max 4 per event)
- **Data**: Non-indexed parameters containing event details
- **Event Signature**: Topic[0] contains keccak256 hash of event signature

### Common Event Signatures (topic_0):
| Event | Signature Hash | Description |
|-------|----------------|-------------|
| Transfer (ERC-20) | 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef | Token transfers |
| Approval (ERC-20) | 0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925 | Token approvals |
| Transfer (ERC-721) | 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef | NFT transfers |
| Swap (Uniswap V2) | 0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822 | DEX swaps |

### Important Relationships:
- **Join with ez_decoded_event_logs**: Use `tx_hash` and `event_index` for simplified decoded data
- **Join with fact_transactions**: Use `tx_hash` for transaction context
- **Join with dim_contracts**: Use `contract_address` for contract metadata

### Event Structure:
- **Indexed Parameters**: Stored in topics[1-3], used for filtering
- **Non-indexed Parameters**: Stored in data field, contains full values
- **Anonymous Events**: Have no topic[0] (rare)

### Sample Queries:

```sql
-- Find all ERC-20 Transfer events in last 24 hours
SELECT 
    block_timestamp,
    tx_hash,
    contract_address,
    topics[1] AS from_address_padded,
    topics[2] AS to_address_padded,
    data AS amount_hex,
    event_index
FROM <blockchain_name>.core.fact_event_logs
WHERE topic_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    AND ARRAY_SIZE(topics) = 3  -- ERC-20 has 3 topics
    AND block_timestamp >= CURRENT_DATE - 1
LIMIT 100;

-- Most active contracts by event count
SELECT 
    contract_address,
    COUNT(*) AS event_count,
    COUNT(DISTINCT tx_hash) AS unique_txs,
    COUNT(DISTINCT topic_0) AS unique_event_types,
    MIN(block_timestamp) AS first_seen,
    MAX(block_timestamp) AS last_seen
FROM <blockchain_name>.core.fact_event_logs
WHERE block_timestamp >= CURRENT_DATE - 7
GROUP BY 1
ORDER BY 2 DESC
LIMIT 50;

-- Event patterns within transactions
SELECT 
    tx_hash,
    COUNT(*) AS events_in_tx,
    COUNT(DISTINCT contract_address) AS contracts_touched,
    ARRAY_AGG(DISTINCT topic_0) AS event_signatures
FROM <blockchain_name>.core.fact_event_logs
WHERE block_timestamp >= CURRENT_DATE - 1
GROUP BY 1
HAVING COUNT(*) > 10
ORDER BY 2 DESC
LIMIT 20;
```

Critical Usage Notes:

Topic Padding: Address parameters in topics are left-padded with zeros to 32 bytes
Data Encoding: Raw data is ABI-encoded and needs decoding for human reading
Event Ordering: Use event_index for chronological order within a transaction
Gas Efficiency: Indexed parameters cost more gas but enable efficient queries

{% enddocs %}

{% docs fact_event_logs_event_index %}
Zero-based sequential position of the event within a transaction's execution.
Key Facts:

Starts at 0 for first event
Increments across all contracts in transaction
Preserves execution order
Essential for deterministic event ordering

Usage Example:

```sql
-- Trace event execution flow
SELECT 
    event_index,
    contract_address,
    topic_0,
    SUBSTRING(data, 1, 10) AS data_preview
FROM <blockchain_name>.core.fact_event_logs
WHERE tx_hash = '0xabc...'
ORDER BY event_index;
```

{% enddocs %}

{% docs fact_event_logs_event_removed %}
Boolean flag indicating if the event was removed due to chain reorganization.
Values:

FALSE: Event is confirmed (vast majority)
TRUE: Event was in a reorganized block

Important: Removed events may indicate:

Chain reorganization occurred
Block was uncle/orphaned
Transaction changed position

Query Usage:

```sql
-- Filter only confirmed events
WHERE event_removed = FALSE

-- Detect reorg activity
SELECT DATE(block_timestamp), COUNT(*)
FROM <blockchain_name>.core.fact_event_logs
WHERE event_removed = TRUE
GROUP BY 1;
```
{% enddocs %}

{% docs fact_event_logs_contract_address %}

Smart contract address that emitted this event.
Key Points:

Always the immediate event emitter
May differ from transaction to_address
Lowercase normalized format
Never NULL for valid events

{% enddocs %}

{% docs fact_event_logs_data %}

Hex-encoded non-indexed event parameters.
Format: 0x-prefixed hex string
Encoding: ABI-encoded based on event signature
Length: Variable (depends on parameters)
Decoding Example:

```sql
-- ERC-20 Transfer amount (assuming standard uint256)
-- data = '0x0000000000000000000000000000000000000000000000000de0b6b3a7640000'
-- Represents: 1000000000000000000 (1e18 = 1 token)

SELECT 
    contract_address,
    -- Convert hex to decimal (conceptual)
    CAST(CONCAT('0x', SUBSTR(data, 67, 64)) AS NUMBER) / 1e18 AS amount
FROM <blockchain_name>.core.fact_event_logs
WHERE topic_0 = '0xddf252ad...' -- Transfer event
    AND LENGTH(data) = 66; -- Single uint256
```
{% enddocs %}

{% docs fact_event_logs_topics %}

Array containing all indexed parameters of the event.
Structure:

topics[0]: Event signature hash (except anonymous events)
topics[1-3]: Indexed parameters (if any)

Example:
[
  "0xddf252ad...", // Transfer(address,address,uint256)
  "0x000000000000000000000000123...", // from (padded)
  "0x000000000000000000000000456..."  // to (padded)
]
Array Access:

```sql
SELECT 
    topics[0] AS event_signature,
    topics[1] AS param1,
    ARRAY_SIZE(topics) AS topic_count
FROM <blockchain_name>.core.fact_event_logs;
```
{% enddocs %}

{% docs fact_event_logs_topic_0 %}

Event signature hash - keccak256 of the event declaration.
Calculation: keccak256("EventName(type1,type2,...)")
Format: 32-byte hex hash
Common Signatures:

```sql
-- ERC-20 Transfer
'0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'

-- ERC-20 Approval  
'0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925'

-- WETH Deposit
'0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c'
```
Usage: Primary filter for event type identification

{% enddocs %}

{% docs fact_event_logs_topic_1 %}

First indexed parameter of the event (if exists).
Common Uses:

'from' address in Transfer events
'owner' in Approval events
Token ID in NFT events

Format: 32-byte value (left-padded for smaller types)
Address Extraction:

```sql
-- Remove padding from address
'0x' || SUBSTR(topic_1, 27, 40) AS from_address
```
{% enddocs %}

{% docs fact_event_logs_topic_2 %}
Second indexed parameter of the event (if exists).
Common Uses:

'to' address in Transfer events
'spender' in Approval events
Additional filter parameters

NULL: When event has fewer than 2 indexed parameters

{% enddocs %}

{% docs fact_event_logs_topic_3 %}

Third indexed parameter of the event (if exists).
Common Uses:

Token ID in ERC-721 Transfer events
Additional indexed values
Custom protocol parameters

Limitations:

Maximum 3 indexed parameters per event
NULL for events with fewer indexed params
Indexed strings/arrays store hash, not value

Example - NFT Transfer:

```sql
SELECT 
    '0x' || SUBSTR(topic_1, 27, 40) AS from_address,
    '0x' || SUBSTR(topic_2, 27, 40) AS to_address,
    topic_3 AS token_id_hex
FROM <blockchain_name>.core.fact_event_logs
WHERE topic_0 = '0xddf252ad...'
    AND ARRAY_SIZE(topics) = 4; -- NFT Transfer has 4 topics
```
{% enddocs %}