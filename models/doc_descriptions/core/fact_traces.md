{% docs fact_traces_table_doc %}

## What

This table contains detailed execution traces of all internal transactions within the EVM. While fact_transactions shows external calls, this table reveals the complete execution flow including contract-to-contract calls, value transfers, and computation paths.

## Key Use Cases

- Analyzing internal contract-to-contract calls and value transfers
- Debugging failed transactions and understanding revert reasons
- Tracking contract deployments (CREATE/CREATE2 operations)
- Understanding DeFi protocol interactions and MEV analysis
- Monitoring delegatecall patterns and proxy contract usage

## Important Relationships

- **Join with fact_transactions**: Use `tx_hash` for transaction context
- **Self-join for trace tree**: Use `tx_hash` and `trace_address` array relationships
- **Join with fact_event_logs**: Match execution flow with events
- **Join with dim_contracts**: Get metadata for interacting contracts

## Commonly-used Fields

- `trace_index`: Sequential execution order within transaction
- `trace_address`: Array showing position in execution tree
- `type`: Operation type (CALL, DELEGATECALL, CREATE, etc.)
- `from_address`: Address initiating this internal call
- `to_address`: Destination address (NULL for contract creation)
- `value`: Native token amount transferred
- `trace_succeeded`: Whether execution completed successfully

## Sample queries

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

{% enddocs %}

{% docs fact_traces_from_address %}

Address that initiated this specific internal call.

Example: '0x7a250d5630b4cf539739df2c5dacb4c659f2488d'

{% enddocs %}

{% docs fact_traces_gas %}

Gas allocated to this specific trace execution.

Example: 250000

{% enddocs %}

{% docs fact_traces_gas_used %}

Actual gas consumed by this trace execution.

Example: 125673

{% enddocs %}

{% docs fact_traces_trace_index %}

Sequential index of trace within the transaction's execution.

Example: 3

{% enddocs %}

{% docs fact_traces_input %}

Hex-encoded input data for this trace (function call data).

Example: '0xa9059cbb0000000000000000000000001234567890123456789012345678901234567890'

{% enddocs %}

{% docs fact_traces_output %}

Hex-encoded output data from trace execution.

Example: '0x0000000000000000000000000000000000000000000000000000000000000001'

{% enddocs %}

{% docs fact_traces_sub_traces %}

Count of immediate child traces spawned by this trace.

Example: 2

{% enddocs %}

{% docs fact_traces_to_address %}

Destination address for this internal call.

Example: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'

{% enddocs %}

{% docs fact_traces_type %}

The type of EVM operation performed.

Example: 'CALL'

{% enddocs %}

{% docs fact_traces_trace_succeeded %}

Boolean indicating if the trace executed successfully.

Example: true

{% enddocs %}

{% docs fact_traces_error_reason %}

Technical reason for trace failure.

Example: 'Out of gas'

{% enddocs %}

{% docs fact_traces_trace_address %}

Array describing the trace's position in the execution tree.

Example: [0, 1, 2]

{% enddocs %}

{% docs fact_traces_revert_reason %}

Human-readable revert message from contract require/revert statements.

Example: 'Insufficient balance'

{% enddocs %}