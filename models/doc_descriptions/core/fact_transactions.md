{% docs fact_transactions_table_doc %}

## What

This table contains comprehensive transaction-level data for EVM blockchains. Each row represents a single transaction with its execution details, gas consumption, and value transfers. This is a high-level table for analyzing on-chain activity, user behavior, and protocol interactions.

## Key Use Cases

- Tracking wallet activity and transaction patterns
- Analyzing gas fee trends and optimization opportunities
- Monitoring smart contract interactions and usage
- Calculating transaction volumes and network revenue
- Detecting MEV, arbitrage, and trading patterns

## Important Relationships

- **Join with fact_blocks**: Use `block_number` for block-level context
- **Join with fact_traces**: Use `tx_hash` for internal transactions
- **Join with fact_event_logs**: Use `tx_hash` for emitted events
- **Join with ez_decoded_event_logs**: Use `tx_hash` for human-readable events
- **Join with dim_contracts**: Use `to_address` for contract metadata

## Commonly-used Fields

- `tx_hash`: Unique transaction identifier
- `from_address`: Transaction sender
- `to_address`: Transaction recipient
- `value`: Native token amount transferred
- `gas_used`: Actual gas consumed
- `gas_price`: Price per gas unit
- `tx_fee`: Total transaction fee in native tokens
- `block_timestamp`: When transaction was included

## Sample queries

```sql
-- Daily transaction statistics by type
SELECT 
    DATE_TRUNC('day', block_timestamp) AS day,
    tx_type,
    COUNT(*) AS tx_count,
    COUNT(DISTINCT from_address) AS unique_senders,
    SUM(tx_fee) AS total_fees_native,
    AVG(gas_used) AS avg_gas_used,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY gas_price) AS median_gas_price
FROM <blockchain_name>.core.fact_transactions
WHERE block_timestamp >= CURRENT_DATE - 30
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;

-- High-value native token transfers
SELECT 
    tx_hash,
    block_timestamp,
    from_address,
    to_address,
    value,
    tx_fee,
    gas_used * gas_price / 1e9 AS gas_cost_gwei
FROM <blockchain_name>.core.fact_transactions
WHERE value > 0
    AND tx_succeeded
    AND block_timestamp >= CURRENT_DATE - 7
ORDER BY value DESC
LIMIT 100;

-- Smart contract interaction patterns
SELECT 
    to_address,
    origin_function_signature,
    COUNT(*) AS interaction_count,
    COUNT(DISTINCT from_address) AS unique_users,
    SUM(tx_fee) AS total_fees_paid
FROM <blockchain_name>.core.fact_transactions
WHERE to_address IN (SELECT address FROM dim_contracts)
    AND block_timestamp >= CURRENT_DATE - 1
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 50;
```

{% enddocs %}

{% docs fact_transactions_cumulative_gas_used %}

Running total of gas consumed by all transactions up to and including this transaction within the block.

Example: 1234567

{% enddocs %}

{% docs fact_transactions_tx_fee %}

Total fee paid for transaction execution in native token units.

Example: 0.002

{% enddocs %}

{% docs fact_transactions_tx_gas_limit %}

Maximum gas units the sender is willing to consume for this transaction.

Example: 150000

{% enddocs %}

{% docs fact_transactions_tx_gas_price %}

Price per gas unit in Gwei (1 Gwei = 1e-9 native token).

Example: 25

{% enddocs %}

{% docs fact_transactions_tx_gas_used %}

Actual gas units consumed by transaction execution.

Example: 89234

{% enddocs %}

{% docs fact_transactions_input_data %}

Encoded data sent with the transaction, containing function calls and parameters.

Example: '0xa9059cbb0000000000000000000000001234567890123456789012345678901234567890'

{% enddocs %}

{% docs fact_transactions_nonce %}

Sequential counter of transactions sent by the from_address.

Example: 42

{% enddocs %}

{% docs fact_transactions_effective_gas_price %}

Actual price paid per gas unit for EIP-1559 transactions, in Gwei.

Example: 23.5

{% enddocs %}

{% docs fact_transactions_max_fee_per_gas %}

Maximum total fee per gas unit sender is willing to pay (EIP-1559), in Gwei.

Example: 50

{% enddocs %}

{% docs fact_transactions_max_priority_fee_per_gas %}

Maximum tip per gas unit for validator (EIP-1559), in Gwei.

Example: 2

{% enddocs %}

{% docs fact_transactions_r %}

R component of ECDSA signature (32 bytes).

Example: '0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef'

{% enddocs %}

{% docs fact_transactions_s %}

S component of ECDSA signature (32 bytes).

Example: '0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890'

{% enddocs %}

{% docs fact_transactions_v %}

Recovery identifier for ECDSA signature.

Example: 27

{% enddocs %}

{% docs fact_transactions_tx_fee_precise %}

Exact transaction fee as string to prevent floating-point precision loss.

Example: '0.002345678901234567'

{% enddocs %}

{% docs fact_transactions_tx_type %}

Transaction envelope type (EIP-2718).

Example: 2

{% enddocs %}

{% docs fact_transactions_mint %}

Minting event data for special transactions.

Example: null

{% enddocs %}

{% docs fact_transactions_source_hash %}

Hash linking L2 transactions to their L1 origin.

Example: '0x9876543210fedcba9876543210fedcba9876543210fedcba9876543210fedcba'

{% enddocs %}

{% docs fact_transactions_eth_value %}

ETH value for cross-chain transactions on L2s.

Example: 0.5

{% enddocs %}

{% docs fact_transactions_l1_fee_precise_raw %}

Raw L1 data availability fee for L2 transactions, in Gwei.

Example: 123456789

{% enddocs %}

{% docs fact_transactions_l1_fee_precise %}

Formatted L1 fee for L2 transactions, in native token units.

Example: '0.000123456789'

{% enddocs %}

{% docs fact_transactions_y_parity %}

Y coordinate parity for signature recovery (EIP-2098).

Example: 1

{% enddocs %}

{% docs fact_transactions_access_list %}

Array of addresses and storage keys for optimized gas costs (EIP-2930).

Example: [{"address": "0x123...", "storageKeys": ["0x456..."]}]

{% enddocs %}

{% docs fact_transactions_token_ratio %}

ETH/MNT price ratio for Mantle network fee calculations.

Example: 1.5

{% enddocs %}

{% docs fact_transactions_l1_base_fee_scalar %}

Multiplier for L1 base fee in L2 fee calculation.

Example: 1500

{% enddocs %}

{% docs fact_transactions_l1_blob_base_fee %}

L1 blob base fee at time of L2 transaction.

Example: 1

{% enddocs %}

{% docs fact_transactions_l1_blob_base_fee_scalar %}

Multiplier for blob base fee in L2 calculations.

Example: 1000

{% enddocs %}

{% docs fact_transactions_authorization_list %}

EIP-7702 authorization entries for EOA delegation.

Example: []

{% enddocs %}

{% docs fact_transactions_operator_fee_scalar %}

OP Stack operator fee multiplier.

Example: 100

{% enddocs %}

{% docs fact_transactions_operator_fee_constant %}

OP Stack fixed operator fee component.

Example: 0

{% enddocs %}

{% docs fact_transactions_timeboosted %}

Arbitrum-specific priority transaction flag.

Example: false

{% enddocs %}

{% docs fact_transactions_blob_versioned_hashes %}

Array of blob commitment hashes for EIP-4844 transactions.

Example: ['0x01234567890abcdef1234567890abcdef1234567890abcdef1234567890abcd']

{% enddocs %}

{% docs fact_transactions_max_fee_per_blob_gas %}

Maximum price sender will pay per blob gas unit.

Example: 3

{% enddocs %}

{% docs fact_transactions_blob_gas_price %}

Actual price paid per blob gas unit.

Example: 1

{% enddocs %}