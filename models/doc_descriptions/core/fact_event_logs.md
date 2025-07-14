{% docs fact_event_logs_table_doc %}

## What

This table contains raw event logs emitted by smart contracts during transaction execution. Each row represents a single event with its topics and data. Events are the primary mechanism for smart contracts to communicate state changes and must be explicitly emitted in contract code.

## Key Use Cases

- Tracking raw blockchain events before decoding
- Filtering events by signature (topic_0) for specific event types
- Analyzing contract activity patterns and event frequencies
- Building custom event decoders for unsupported contracts
- Monitoring specific addresses via indexed parameters

## Important Relationships

- **Join with ez_decoded_event_logs**: Use `tx_hash` and `event_index` for simplified decoded data
- **Join with fact_transactions**: Use `tx_hash` for transaction context
- **Join with dim_contracts**: Use `contract_address` for contract metadata

## Commonly-used Fields

- `topic_0`: Event signature hash for filtering event types
- `contract_address`: Smart contract that emitted the event
- `topics`: Array of indexed parameters (max 4)
- `data`: Hex-encoded non-indexed parameters
- `event_index`: Sequential position within transaction
- `tx_hash`: Transaction containing this event

## Sample queries

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

{% enddocs %}

{% docs fact_event_logs_event_index %}

Zero-based sequential position of the event within a transaction's execution.

Example: 5

{% enddocs %}

{% docs fact_event_logs_event_removed %}

Boolean flag indicating if the event was removed due to chain reorganization.

Example: false

{% enddocs %}

{% docs fact_event_logs_contract_address %}

Smart contract address that emitted this event.

Example: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'

{% enddocs %}

{% docs fact_event_logs_data %}

Hex-encoded non-indexed event parameters.

Example: '0x0000000000000000000000000000000000000000000000000de0b6b3a7640000'

{% enddocs %}

{% docs fact_event_logs_topics %}

Array containing all indexed parameters of the event.

Example: ['0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef', '0x0000000000000000000000001234567890123456789012345678901234567890']

{% enddocs %}

{% docs fact_event_logs_topic_0 %}

Event signature hash - keccak256 of the event declaration.

Example: '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'

{% enddocs %}

{% docs fact_event_logs_topic_1 %}

First indexed parameter of the event (if exists).

Example: '0x0000000000000000000000001234567890123456789012345678901234567890'

{% enddocs %}

{% docs fact_event_logs_topic_2 %}

Second indexed parameter of the event (if exists).

Example: '0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd'

{% enddocs %}

{% docs fact_event_logs_topic_3 %}

Third indexed parameter of the event (if exists).

Example: '0x0000000000000000000000000000000000000000000000000000000000000001'

{% enddocs %}