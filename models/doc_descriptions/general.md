{% docs general_block_number %}

Sequential counter representing the position of a block in the blockchain since genesis (block 0).

**Key Facts**:
- Immutable once finalized
- Primary ordering mechanism for blockchain data
- Increments by 1 for each new block
- Used as a proxy for time in many analyses

**Usage in Queries**:
```sql
-- Recent data
WHERE block_number >= (SELECT MAX(block_number) - 1000 FROM fact_blocks)

-- Historical analysis
WHERE block_number BETWEEN 15000000 AND 16000000

-- Join across tables
JOIN <blockchain_name>.core.fact_event_logs USING (block_number)
```

**Important**: Block numbers are chain-specific. Block 15000000 on Ethereum ≠ block 15000000 on Polygon.

{% enddocs %}

{% docs general_tx_hash %}

Unique 66-character identifier for the transaction.

**Format**: 0x + 64 hexadecimal characters

**Usage**:
- Primary key for transaction lookups
- Join key for traces, logs, and token transfers
- Immutable once confirmed

**Example**: `0x5c504ed432cb51138bcf09aa5e8a410dd4a1e204ef84bfed1be16dfba1b22060`

{% enddocs %}

{% docs general_block_timestamp %}

UTC timestamp when the block was produced by validators/miners.

**Format**: TIMESTAMP_NTZ (no timezone)
**Precision**: Second-level accuracy
**Reliability**:
- Set by block producer
- Can have minor variations (±15 seconds)
- Always increasing (newer blocks = later timestamps)

**Best Practices**:
```sql
-- Time-based filtering (most efficient)
WHERE block_timestamp >= DATEADD('day', -7, CURRENT_TIMESTAMP)

-- Hourly aggregations
DATE_TRUNC('hour', block_timestamp) AS hour

-- UTC date extraction
DATE(block_timestamp) AS block_date
```

**Note**: Use for time-series analysis, but be aware that block production rates vary by chain.

{% enddocs %}

{% docs general_from_address %}

The externally-owned account (EOA) or contract address that initiated the transaction.

**Key Points**:
- Always 42 characters (0x + 40 hex chars)
- Lowercase normalized in all tables
- Cannot be NULL for valid transactions
- For contract creation: sender of creation transaction

**Common Patterns**:
- EOA → EOA: Simple transfer
- EOA → Contract: User interaction
- Contract → Contract: Internal calls (see fact_traces)
- Known addresses: Exchange hot wallets, protocol deployers

**Query Examples**:
```sql
-- User activity analysis
SELECT from_address, COUNT(*) as tx_count
FROM <blockchain_name>.core.fact_transactions
WHERE block_timestamp >= CURRENT_DATE - 30
GROUP BY 1
ORDER BY 2 DESC;

-- New user detection
SELECT DISTINCT from_address
FROM <blockchain_name>.core.fact_transactions t1
WHERE NOT EXISTS (
    SELECT 1 FROM <blockchain_name>.core.fact_transactions t2 
    WHERE t2.from_address = t1.from_address 
    AND t2.block_number < t1.block_number
);
```

{% enddocs %}

{% docs general_to_address %}

The destination address for the transaction - either an EOA or contract address.

**Special Cases**:
- NULL: Contract creation transaction
- Contract address: Interacting with smart contract
- EOA address: Simple transfer or receiving funds

**Important Patterns**:
```sql
-- Contract deployments
WHERE to_address IS NULL

-- Popular contracts
SELECT to_address, COUNT(*) as interactions
FROM <blockchain_name>.core.fact_transactions
WHERE to_address IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC;

-- Direct transfers only
WHERE to_address NOT IN (SELECT address FROM dim_contracts)
```

**Note**: For token transfers, this is the token contract, not the recipient. See ez_token_transfers tables for recipient.

{% enddocs %}

{% docs general_pk %}

Primary key - unique identifier for each row ensuring data integrity.

**Format**: Usually VARCHAR containing composite key generated using MD5 hash of the relevant columns.
**Example**: MD5(block_number, tx_hash, trace_index)

**Usage**:
- Deduplication in incremental loads
- Join operations for data quality checks
- Troubleshooting specific records

**Important**: Implementation varies by table - check table-specific documentation.

{% enddocs %}

{% docs general_inserted_timestamp %}

UTC timestamp when the record was first added to the Flipside database.

**Format**: TIMESTAMP_NTZ

**Use Cases**:
- Data freshness monitoring
- Incremental processing markers
- Debugging data pipeline issues
- SLA tracking

**Query Example**:
```sql
-- Check data latency
SELECT 
    DATE_TRUNC('hour', block_timestamp) as block_hour,
    DATE_TRUNC('hour', inserted_timestamp) as insert_hour,
    AVG(TIMESTAMPDIFF('minute', block_timestamp, inserted_timestamp)) as avg_latency_minutes
FROM <blockchain_name>.core.fact_transactions
WHERE block_timestamp >= CURRENT_DATE - 1
GROUP BY 1, 2;
```

{% enddocs %}

{% docs general_modified_timestamp %}

UTC timestamp of the most recent update to this record.

**Format**: TIMESTAMP_NTZ

**Triggers for Updates**:
- Data corrections
- Enrichment additions
- Reprocessing for accuracy
- Schema migrations

**Monitoring Usage**:
```sql
-- Recently modified records
SELECT *
FROM <blockchain_name>.core.fact_transactions
WHERE modified_timestamp > inserted_timestamp
AND modified_timestamp >= CURRENT_DATE - 1;

-- Data quality tracking
SELECT 
    DATE(modified_timestamp) as mod_date,
    COUNT(*) as records_updated,
    COUNT(DISTINCT block_number) as blocks_affected
FROM <blockchain_name>.core.fact_transactions
WHERE modified_timestamp > inserted_timestamp
GROUP BY 1
ORDER BY 1 DESC;
```

{% enddocs %}

{% docs general_value_precise_raw %}

String representation of numeric values preserving exact precision without any adjustments.

**Format**: VARCHAR containing numeric string
**Purpose**: Prevents floating-point precision loss due to snowflake limitations
**Contains**: Raw blockchain values (usually in smallest unit)

**Example Values**:
- "1000000000000000000" = 1 ETH in Wei
- "50000000" = 50 USDC (6 decimals)

**Usage**:
```sql
-- Exact value comparisons
WHERE value_precise_raw = '1000000000000000000'

-- Conversion with precision
CAST(value_precise_raw AS NUMERIC(38,0)) / POW(10, 18) AS value_decimal
```

{% enddocs %}

{% docs general_value_precise %}

String representation of numeric values adjusted for human readability while maintaining precision.

**Format**: VARCHAR containing decimal string
**Adjustments**: Converted from smallest unit to standard unit
**Purpose**: Human-readable values without precision loss

**Example Values**:
- "1.0" = 1 ETH (converted from Wei)
- "50.0" = 50 USDC (converted from 6 decimal places)

**Best Practices**:
```sql
-- Safe numeric operations
CAST(value_precise AS NUMERIC(38,18))

-- Filtering large values
WHERE CAST(value_precise AS NUMERIC(38,18)) > 1000

-- Aggregations
SUM(CAST(value_precise AS NUMERIC(38,18))) AS total_value
```

{% enddocs %}

{% docs general_value_hex %}

Hexadecimal representation of transaction values as provided by the blockchain RPC.

**Format**: 0x-prefixed hex string
**Example**: "0xde0b6b3a7640000" = 1 ETH in Wei

**Use Cases**:
- Debugging RPC responses
- Verifying data transformations
- Handling special encoding cases

**Conversion Example**:
- Hex to decimal (conceptual - use built-in conversions)
- 0xde0b6b3a7640000 = 1000000000000000000 Wei = 1 ETH

**Note**: Most queries should use value or value_precise fields instead.

{% enddocs %}

{% docs general_origin_function_signature %}

Function signature (first 4 bytes) of the called method.

**Format**: 0x + 8 hex characters

**Common Signatures**:
- 0xa9059cbb: transfer(address,uint256)
- 0x095ea7b3: approve(address,uint256)
- 0x23b872dd: transferFrom(address,address,uint256)

**Note**: NULL for simple transfers or invalid calls

{% enddocs %}

{% docs general_tx_position %}

Zero-indexed position of transaction within its block.

**Insights**:
- Position 0: First transaction in block
- MEV bots often target early positions
- Bundle transactions appear consecutively
- Useful for analyzing transaction ordering

{% enddocs %}

{% docs general_value %}

Amount of native tokens transferred, in token units (not Wei).

**Key Points**:
- 0 for most contract interactions
- >0 for native token transfers or payable functions
- Already converted from Wei (divided by 1e18)
- Use value_precise for exact amounts

**Example Query**:
```sql
-- Daily native token transfer volume
SELECT 
    DATE_TRUNC('day', block_timestamp) AS day,
    SUM(value) AS total_transferred,
    COUNT(*) AS transfer_count
FROM <blockchain_name>.core.fact_transactions
WHERE value > 0 AND tx_succeeded
GROUP BY 1;
```

{% enddocs %}

{% docs general_tx_succeeded %}

Boolean indicator of transaction success.

**Values**:
- TRUE: Transaction executed successfully
- FALSE: Transaction failed/reverted

{% enddocs %}

{% docs general_event_index %}

Zero-based sequential position of the event within a transaction's execution.

**Key Facts**:
- Starts at 0 for first event
- Increments across all contracts in transaction
- Preserves execution order
- Essential for deterministic event ordering

**Usage Example**:
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

{% docs general_contract_address %}

Smart contract address that emitted this event or received the transaction.

**Key Points**:
- Always the immediate event emitter for logs
- May differ from transaction to_address
- Lowercase normalized format
- Never NULL for valid events

{% enddocs %}

{% docs general_event_name %}

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