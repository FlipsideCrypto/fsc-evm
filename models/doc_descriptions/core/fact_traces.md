{% docs fact_traces_table_doc %}

## Table: fact_traces

This table contains detailed execution traces of all internal transactions within the EVM. While fact_transactions shows external calls, this table reveals the complete execution flow including contract-to-contract calls, value transfers, and computation paths. Essential for understanding DeFi protocols, MEV analysis, and debugging failed transactions.

### Key Concepts:
- **Internal Transactions**: Contract-to-contract calls not visible in fact_transactions
- **Trace Tree**: Hierarchical execution flow with parent-child relationships
- **Call Types**: Different execution contexts (CALL, DELEGATECALL, STATICCALL, CREATE)
- **Gas Propagation**: How gas flows through nested calls

### Trace Types:
| Type | Description | Value Transfer | Storage Context |
|------|-------------|----------------|-----------------|
| CALL | Standard contract call | Yes | Called contract |
| DELEGATECALL | Execute in caller's context | No | Calling contract |
| STATICCALL | Read-only call | No | Called contract |
| CREATE | Contract deployment | Yes | New contract |
| CREATE2 | Deterministic deployment | Yes | New contract |
| SELFDESTRUCT | Contract destruction | Yes | N/A |

### Important Relationships:
- **Join with fact_transactions**: Use `tx_hash` for transaction context
- **Self-join for trace tree**: Use `tx_hash` and `trace_address` array relationships
- **Join with fact_event_logs**: Match execution flow with events
- **Join with dim_contracts**: Get metadata for interacting contracts

### Sample Queries:

```sql
-- Analyze internal ETH transfers
SELECT 
    block_timestamp,
    tx_hash,
    trace_index,
    type,
    from_address,
    to_address,
    value,
    gas_used,
    trace_succeeded
FROM <blockchain_name>.core.fact_traces
WHERE value > 0
    AND type = 'CALL'
    AND trace_succeeded
    AND block_timestamp >= CURRENT_DATE - 1
ORDER BY value DESC
LIMIT 100;

-- Find failed internal transactions with reasons
SELECT 
    tx_hash,
    trace_index,
    from_address,
    to_address,
    type,
    error_reason,
    revert_reason,
    gas,
    gas_used
FROM <blockchain_name>.core.fact_traces
WHERE NOT trace_succeeded
    AND block_timestamp >= CURRENT_DATE - 1
    AND error_reason IS NOT NULL
LIMIT 50;

-- Trace execution depth analysis
SELECT 
    tx_hash,
    MAX(ARRAY_SIZE(trace_address)) AS max_depth,
    COUNT(*) AS total_traces,
    SUM(CASE WHEN trace_succeeded THEN 0 ELSE 1 END) AS failed_traces,
    SUM(value) AS total_value_transferred
FROM <blockchain_name>.core.fact_traces
WHERE block_timestamp >= CURRENT_DATE - 1
GROUP BY 1
HAVING MAX(ARRAY_SIZE(trace_address)) > 3
ORDER BY 2 DESC;

-- Contract interaction patterns
WITH contract_calls AS (
    SELECT 
        from_address AS caller,
        to_address AS callee,
        COUNT(*) AS call_count,
        SUM(value) AS total_value
    FROM <blockchain_name>.core.fact_traces
    WHERE type IN ('CALL', 'DELEGATECALL')
        AND block_timestamp >= CURRENT_DATE - 7
    GROUP BY 1, 2
)
SELECT * FROM contract_calls
WHERE call_count > 100
ORDER BY call_count DESC;
```

### Critical Usage Notes:
- **Gas Accounting**: Parent trace gas includes all child trace gas
- **Value Transfers**: Only CALL and CREATE can transfer native tokens
- **Trace Ordering**: Use trace_index for execution order, trace_address for hierarchy
- **Failed Traces**: May still consume gas and emit events before failure

{% enddocs %}

{% docs fact_traces_from_address %}

Address that initiated this specific internal call.

**Important Distinctions**:
- NOT always the transaction sender (from_address in fact_transactions)
- For nested calls: the immediate calling contract
- For first trace: matches transaction from_address

**Tracing Call Paths**:
```sql
-- Trace call delegation
SELECT 
    trace_index,
    trace_address,
    from_address,
    to_address,
    type
FROM <blockchain_name>.core.fact_traces
WHERE tx_hash = '0xabc...'
ORDER BY trace_index;
```

{% enddocs %}

{% docs fact_traces_gas %}

Gas allocated to this specific trace execution.

**Gas Flow**:
- Parent allocates gas to children
- Unused gas returns to parent
- Failed calls consume all allocated gas
- Top-level trace gets transaction gas_limit

**Analysis Pattern**:
```sql
-- Gas efficiency by call type
SELECT 
    type,
    AVG(gas_used::FLOAT / gas) AS avg_efficiency,
    COUNT(*) AS trace_count
FROM <blockchain_name>.core.fact_traces
WHERE gas > 0
    AND trace_succeeded
GROUP BY 1;
```

{% enddocs %}

{% docs fact_traces_gas_used %}

Actual gas consumed by this trace execution.

**Includes**:
- Computation costs
- Storage operations
- Sub-trace gas consumption
- Base costs for call type

**Note**: Failed traces may show partial consumption before failure point

{% enddocs %}

{% docs fact_traces_trace_index %}

Sequential index of trace within the transaction's execution.

**Properties**:
- Starts at 0 (usually the main call)
- Increments for each trace
- Reflects execution order (depth-first)
- NOT the same as trace_address position

**Ordering Example**:
```sql
SELECT 
    trace_index,
    trace_address,
    type,
    from_address,
    to_address
FROM <blockchain_name>.core.fact_traces
WHERE tx_hash = '0xabc...'
ORDER BY trace_index;
```

{% enddocs %}

{% docs fact_traces_input %}

Hex-encoded input data for this trace (function call data).

**Structure**:
- First 10 chars: Function selector (0x + 8 hex)
- Remaining: ABI-encoded parameters
- Empty (0x): ETH transfer or fallback

**Common Patterns**:
```sql
-- Identify function calls
SELECT 
    SUBSTRING(input, 1, 10) AS function_sig,
    COUNT(*) AS call_count
FROM <blockchain_name>.core.fact_traces
WHERE LENGTH(input) > 10
    AND block_timestamp >= CURRENT_DATE - 1
GROUP BY 1
ORDER BY 2 DESC;
```

{% enddocs %}

{% docs fact_traces_output %}

Hex-encoded output data from trace execution.

**Contents**:
- Function return values (ABI-encoded)
- Empty (0x) for no return
- Error data for failed calls

**Usage**:
- Verify computation results
- Extract return values
- Debug failures

**Decoding Example**:
```sql
-- Check for successful outputs
SELECT 
    trace_index,
    CASE 
        WHEN output = '0x' THEN 'No Return'
        WHEN trace_succeeded = FALSE THEN 'Failed'
        ELSE 'Has Output'
    END AS output_type,
    LENGTH(output) AS output_length
FROM <blockchain_name>.core.fact_traces
WHERE tx_hash = '0xabc...';
```

{% enddocs %}

{% docs fact_traces_sub_traces %}

Count of immediate child traces spawned by this trace.

**Interpretation**:
- 0: Leaf trace (no further calls)
- 1+: Parent trace with nested calls
- Helps understand call complexity

**Tree Analysis**:
```sql
-- Find complex multi-call transactions
SELECT 
    tx_hash,
    SUM(sub_traces) AS total_subcalls,
    MAX(sub_traces) AS max_immediate_subcalls,
    COUNT(*) AS trace_count
FROM <blockchain_name>.core.fact_traces
WHERE block_timestamp >= CURRENT_DATE - 1
GROUP BY 1
HAVING SUM(sub_traces) > 10
ORDER BY 2 DESC;
```

{% enddocs %}

{% docs fact_traces_to_address %}

Destination address for this internal call.

**Special Cases**:
- NULL: Contract creation (CREATE/CREATE2)
- Contract address: Smart contract interaction
- EOA address: Native token transfer
- 0x0: Failed contract creation

**Pattern Detection**:
```sql
-- Identify contract deployments
SELECT * FROM <blockchain_name>.core.fact_traces
WHERE to_address IS NULL
    AND type IN ('CREATE', 'CREATE2')
    AND trace_succeeded;
```

{% enddocs %}

{% docs fact_traces_type %}

The type of EVM operation performed.

**Core Types**:
- **CALL**: Standard call with context switch
- **DELEGATECALL**: Execute in caller's storage
- **STATICCALL**: Read-only call (no state changes)
- **CREATE**: Deploy new contract
- **CREATE2**: Deploy with deterministic address
- **SELFDESTRUCT**: Destroy contract and send funds

**Security Implications**:
```sql
-- Monitor DELEGATECALL usage (proxy patterns)
SELECT 
    DATE(block_timestamp) AS day,
    COUNT(*) AS delegatecall_count,
    COUNT(DISTINCT from_address) AS unique_callers
FROM <blockchain_name>.core.fact_traces
WHERE type = 'DELEGATECALL'
    AND trace_succeeded
GROUP BY 1
ORDER BY 1 DESC;
```

{% enddocs %}

{% docs fact_traces_trace_succeeded %}

Boolean indicating if the trace executed successfully.

**Values**:
- TRUE: Execution completed without revert
- FALSE: Execution reverted or failed

**Important Notes**:
- Failed traces still consume gas
- Child success doesn't guarantee parent success
- Check error_reason for failure details

**Failure Analysis**:
```sql
-- Success rate by trace type
SELECT 
    type,
    COUNT(*) AS total,
    SUM(CASE WHEN trace_succeeded THEN 1 ELSE 0 END) AS succeeded,
    AVG(CASE WHEN trace_succeeded THEN 1 ELSE 0 END) AS success_rate
FROM <blockchain_name>.core.fact_traces
WHERE block_timestamp >= CURRENT_DATE - 7
GROUP BY 1;
```

{% enddocs %}

{% docs fact_traces_error_reason %}

Technical reason for trace failure.

**Common Values**:
- 'Out of gas': Insufficient gas provided
- 'Revert': Explicit revert in contract
- 'Invalid instruction': Bad opcode
- 'Stack underflow/overflow': Stack errors
- NULL: Successful execution

**Debugging Usage**:
```sql
-- Common failure reasons
SELECT 
    error_reason,
    COUNT(*) AS occurrences,
    AVG(gas_used) AS avg_gas_consumed
FROM <blockchain_name>.core.fact_traces
WHERE NOT trace_succeeded
    AND error_reason IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC;
```

{% enddocs %}

{% docs fact_traces_trace_address %}

Array describing the trace's position in the execution tree.

**Format**: Integer array (e.g., [0,1,2])

**Interpretation**:
- [] or [0]: Top-level trace
- [0,1]: Second child of first trace
- [0,1,2]: Third child of [0,1]

**Tree Navigation**:
```sql
-- Find parent-child relationships
WITH trace_tree AS (
    SELECT 
        tx_hash,
        trace_index,
        trace_address,
        ARRAY_SLICE(trace_address, 0, ARRAY_SIZE(trace_address)-1) AS parent_address
    FROM <blockchain_name>.core.fact_traces
    WHERE ARRAY_SIZE(trace_address) > 0
)
SELECT 
    child.trace_index AS child_index,
    parent.trace_index AS parent_index,
    child.type AS child_type,
    parent.type AS parent_type
FROM trace_tree child
LEFT JOIN trace_tree parent
    ON child.tx_hash = parent.tx_hash
    AND child.parent_address = parent.trace_address
WHERE child.tx_hash = '0xabc...';
```

{% enddocs %}

{% docs fact_traces_revert_reason %}

Human-readable revert message from contract require/revert statements.

**Examples**:
- "Insufficient balance"
- "Transfer amount exceeds allowance"
- "Ownable: caller is not the owner"
- Custom protocol-specific messages

**NULL When**:
- Execution succeeded
- No revert message provided
- Out of gas failures

**Error Analysis**:
```sql
-- Common revert reasons by protocol
SELECT 
    to_address,
    revert_reason,
    COUNT(*) AS occurrences
FROM <blockchain_name>.core.fact_traces
WHERE revert_reason IS NOT NULL
    AND block_timestamp >= CURRENT_DATE - 7
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 50;
```

{% enddocs %}