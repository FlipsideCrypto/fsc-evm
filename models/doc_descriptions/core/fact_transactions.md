{% docs fact_transactions_table_doc %}

## Table: fact_transactions

This table contains comprehensive transaction-level data for EVM blockchains. Each row represents a single transaction with its execution details, gas consumption, and value transfers. This is a high-level table for analyzing on-chain activity, user behavior, and protocol interactions.

### Key Use Cases:
- Track wallet activity and transaction patterns
- Analyze gas fee trends and optimization opportunities
- Monitor smart contract interactions and usage
- Calculate transaction volumes and network revenue
- Detect MEV, arbitrage, and trading patterns

### Native Token Mapping:
| Blockchain | Native Token | Decimals |
|------------|--------------|----------|
| ETHEREUM   | ETH          | 18       |
| BINANCE    | BNB          | 18       |
| POLYGON    | POL          | 18       |
| AVALANCHE  | AVAX         | 18       |
| ARBITRUM   | ETH          | 18       |
| OPTIMISM   | ETH          | 18       |
| GNOSIS     | xDAI         | 18       |
| BASE       | ETH          | 18       |
| MANTLE     | MNT          | 18       |
| SCROLL     | ETH          | 18       |
| BOB        | ETH          | 18       |
| BOBA       | ETH          | 18       |
| CORE       | ETH          | 18       |
| INK        | ETH          | 18       |
| RONIN      | ETH          | 18       |
| SWELL      | ETH          | 18       |

### Important Relationships:
- **Join with fact_blocks**: Use `block_number` for block-level context
- **Join with fact_traces**: Use `tx_hash` for internal transactions
- **Join with fact_event_logs**: Use `tx_hash` for emitted events
- **Join with ez_decoded_event_logs**: Use `tx_hash` for human-readable events
- **Join with dim_contracts**: Use `to_address` for contract metadata

### Transaction Types:
- **Type 0**: Legacy transactions (pre-EIP-2718)
- **Type 1**: Access list transactions (EIP-2930)
- **Type 2**: Dynamic fee transactions (EIP-1559)
- **Type 3**: Blob transactions (EIP-4844)

### Sample Queries:

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

### Critical Fields for Analysis:
- **tx_hash**: Unique identifier for joining with other tables
- **block_timestamp**: Essential for time-series analysis
- **from_address / to_address**: Track fund flows and interactions
- **value**: Native token transfers at the transaction level. Note: All native transfers will be found in the `fact_traces` table.
- **gas_used * gas_price**: Actual transaction cost calculation
- **tx_succeeded**: Filter for successful transactions only
- **input_data**: Contains function calls and parameters for contract interactions

{% enddocs %}

{% docs fact_transactions_cumulative_gas_used %}

Running total of gas consumed by all transactions up to and including this transaction within the block.

**Usage**:
- Last transaction's cumulative_gas_used = block's total gas_used
- Calculate transaction's impact: current cumulative - previous cumulative
- Useful for MEV analysis and transaction ordering studies

{% enddocs %}

{% docs fact_transactions_tx_fee %}

Total fee paid for transaction execution in native token units.

**Calculation**:
- Legacy (Type 0): `gas_used * gas_price / 1e18`
- EIP-1559 (Type 2): `gas_used * effective_gas_price / 1e18`
- Includes L1 fees for L2 chains where applicable

**Important**: Already converted to native token units (not Wei/Gwei)

{% enddocs %}

{% docs fact_transactions_tx_gas_limit %}

Maximum gas units the sender is willing to consume for this transaction.

**Key Points**:
- Must be ≥ actual gas_used or transaction fails
- Standard ETH transfer: 21,000 gas
- Complex operations require higher limits
- Unused gas is refunded

{% enddocs %}

{% docs fact_transactions_tx_gas_price %}

Price per gas unit in Gwei (1 Gwei = 1e-9 native token).

**Context**:
- Type 0 & 1: Exact price paid per gas unit
- Type 2: Maximum price, actual price is effective_gas_price
- Market indicator: Higher = network congestion

{% enddocs %}

{% docs fact_transactions_tx_gas_used %}

Actual gas units consumed by transaction execution.

**Insights**:
- 21,000: Simple native token transfer
- Higher values: Contract interactions
- Compare to gas_limit for optimization opportunities
- Failed transactions still consume gas

{% enddocs %}

{% docs fact_transactions_input_data %}

Encoded data sent with the transaction, containing function calls and parameters.

**Structure**:
- First 10 chars (including 0x): Function selector
- Remaining: ABI-encoded parameters
- Empty (0x): Simple value transfer

**Example Uses**:
- `0x095ea7b3...`: ERC-20 approve
- `0xa9059cbb...`: ERC-20 transfer
- `0x7ff36ab5...`: Uniswap swapExactETHForTokens

{% enddocs %}

{% docs fact_transactions_nonce %}

Sequential counter of transactions sent by the from_address.

**Key Facts**:
- Starts at 0 for new addresses
- Must be sequential (gaps cause pending)
- Prevents replay attacks
- Reset never occurs

**Query Example**:
```sql
-- Find address transaction count
SELECT from_address, MAX(nonce) + 1 as tx_count
FROM <blockchain_name>.core.fact_transactions
GROUP BY from_address;
```

{% enddocs %}

{% docs fact_transactions_effective_gas_price %}

Actual price paid per gas unit for EIP-1559 transactions, in Gwei.

**Calculation**: `base_fee_per_gas + priority_fee`
**Relationship**: `effective_gas_price <= max_fee_per_gas`
**Usage**: Calculate exact transaction costs for Type 2 transactions

{% enddocs %}

{% docs fact_transactions_max_fee_per_gas %}

Maximum total fee per gas unit sender is willing to pay (EIP-1559), in Gwei.

**Components**: Includes both base fee and priority fee
**NULL**: For legacy transaction types
**Best Practice**: Set higher than expected base fee to ensure inclusion

{% enddocs %}

{% docs fact_transactions_max_priority_fee_per_gas %}

Maximum tip per gas unit for validator (EIP-1559), in Gwei.

**Purpose**: Incentivizes validators to include transaction
**Typical Values**:
- 1-2 Gwei: Normal priority
- >10 Gwei: High priority/MEV

{% enddocs %}

{% docs fact_transactions_r %}

R component of ECDSA signature (32 bytes).

**Usage**:
- Transaction authentication
- Signature verification
- Forensic analysis

{% enddocs %}

{% docs fact_transactions_s %}

S component of ECDSA signature (32 bytes).

**Usage**: Combined with r and v for complete signature verification

{% enddocs %}

{% docs fact_transactions_v %}

Recovery identifier for ECDSA signature.

**Values**:
- 27-28: Legacy mainnet
- 35+: EIP-155 replay protection
- Encodes chain_id for replay protection

{% enddocs %}

{% docs fact_transactions_tx_fee_precise %}

Exact transaction fee as string to prevent floating-point precision loss.

**Usage**: Critical for accounting and reconciliation where exact values matter
**Format**: String representation of decimal value

{% enddocs %}

{% docs fact_transactions_tx_type %}

Transaction envelope type (EIP-2718).

**Types**:
- 0: Legacy (pre-EIP-2718)
- 1: Access list (EIP-2930)
- 2: Dynamic fee (EIP-1559)
- 3: Blob (EIP-4844)

**Impact**: Determines fee calculation and available fields

{% enddocs %}

{% docs fact_transactions_mint %}

Minting event data for special transactions.

**Applies To**:
- Coinbase transactions
- L2 sequencer rewards
- Protocol-specific minting

{% enddocs %}

{% docs fact_transactions_source_hash %}

Hash linking L2 transactions to their L1 origin.

**L2-Specific**: Used for deposit transactions and cross-layer tracing
**NULL**: For standard L1 transactions

{% enddocs %}

{% docs fact_transactions_eth_value %}

ETH value for cross-chain transactions on L2s.

**Context**: Some L2s distinguish between native token and ETH values
**Example**: Mantle has both MNT (native) and ETH values

{% enddocs %}

{% docs fact_transactions_l1_fee_precise_raw %}

Raw L1 data availability fee for L2 transactions, in Gwei.

**L2-Specific**: Cost of posting transaction data to L1
**Calculation**: Based on L1 gas price and transaction size

{% enddocs %}

{% docs fact_transactions_l1_fee_precise %}

Formatted L1 fee for L2 transactions, in native token units.

**Usage**: Add to execution fee for total L2 transaction cost
**NULL**: For L1 transactions

{% enddocs %}

{% docs fact_transactions_y_parity %}

Y coordinate parity for signature recovery (EIP-2098).

**Values**: 0 or 1
**Purpose**: Compact signature representation

{% enddocs %}

{% docs fact_transactions_access_list %}

Array of addresses and storage keys for optimized gas costs (EIP-2930).

**Structure**: `[{address, storageKeys[]}, ...]`
**Benefit**: Pre-declares state access for gas savings

{% enddocs %}

{% docs fact_transactions_token_ratio %}

ETH/MNT price ratio for Mantle network fee calculations.

**Mantle-Specific**: Adjusts fees based on token price
**Updates**: Set by protocol governance

{% enddocs %}

{% docs fact_transactions_l1_base_fee_scalar %}

Multiplier for L1 base fee in L2 fee calculation.

**OP Stack**: Adjusts L1 cost based on network conditions
**Range**: Typically 1000-2000 (divided by 1e6)

{% enddocs %}

{% docs fact_transactions_l1_blob_base_fee %}

L1 blob base fee at time of L2 transaction.

**Post-4844**: Cost per blob gas unit on L1
**Impact**: Affects L2 data availability costs

{% enddocs %}

{% docs fact_transactions_l1_blob_base_fee_scalar %}

Multiplier for blob base fee in L2 calculations.

**Purpose**: Adjusts blob costs for L2 economics
**NULL**: Pre-4844 or non-blob transactions

{% enddocs %}

{% docs fact_transactions_authorization_list %}

EIP-7702 authorization entries for EOA delegation.

**Structure**: Array of delegations allowing contracts to act for EOAs
**Use Case**: Account abstraction and smart wallets

{% enddocs %}

{% docs fact_transactions_operator_fee_scalar %}

OP Stack operator fee multiplier.

**Purpose**: Protocol revenue mechanism
**Calculation**: Part of L2 operator fee formula

{% enddocs %}

{% docs fact_transactions_operator_fee_constant %}

OP Stack fixed operator fee component.

**Added To**: Scaled operator fees
**Revenue**: Goes to L2 operator/treasury

{% enddocs %}

{% docs fact_transactions_timeboosted %}

Arbitrum-specific priority transaction flag.

**TRUE**: Transaction paid for priority inclusion
**Impact**: Faster inclusion, higher fees

{% enddocs %}

{% docs fact_transactions_blob_versioned_hashes %}

Array of blob commitment hashes for EIP-4844 transactions.

**Format**: Versioned 32-byte hashes
**Usage**: Links to blob data without storing on-chain
**L2 Benefit**: Reduced data availability costs

{% enddocs %}

{% docs fact_transactions_max_fee_per_blob_gas %}

Maximum price sender will pay per blob gas unit.

**EIP-4844**: Separate fee market for blob data
**NULL**: Non-blob transactions

{% enddocs %}

{% docs fact_transactions_blob_gas_price %}

Actual price paid per blob gas unit.

**Determined By**: Blob fee market dynamics
**Usage**: Calculate total blob costs
**Benefit**: Cheaper L2 data availability

{% enddocs %}