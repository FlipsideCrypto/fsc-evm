{% docs evm_blocks_table_doc %}

## Table: fact_blocks

This table contains block-level data for EVM blockchains, providing a complete record of all blocks produced on the chain. Each row represents one block with its associated metadata, transactions, and gas metrics.

### Key Use Cases:
- Analyze gas price trends and network congestion over time
- Track block production rates and miner/validator performance
- Monitor network upgrades and their impact (e.g., EIP-1559, EIP-4844)
- Calculate transaction throughput and network utilization
- Identify uncle blocks and chain reorganizations

### Important Relationships:
- **Join with fact_transactions**: Use `block_number` to analyze all transactions within a block
- **Join with fact_traces**: Use `block_number` to examine internal transactions
- **Join with fact_event_logs**: Use `block_number` to find all events emitted in a block

### Sample Queries:

```sql
-- Average gas price and utilization by hour
SELECT 
    DATE_TRUNC('hour', block_timestamp) AS hour,
    AVG(gas_used / gas_limit) AS avg_gas_utilization,
    AVG(base_fee_per_gas) AS avg_base_fee,
    COUNT(*) AS blocks_count,
    SUM(tx_count) AS total_transactions
FROM fact_blocks
WHERE block_timestamp >= CURRENT_DATE - 7
GROUP BY 1
ORDER BY 1 DESC;

-- Identify high-value blocks by total gas used
SELECT 
    block_number,
    block_timestamp,
    tx_count,
    gas_used,
    gas_limit,
    (gas_used::FLOAT / gas_limit) AS utilization_rate,
    miner
FROM fact_blocks
WHERE block_timestamp >= CURRENT_DATE - 1
ORDER BY gas_used DESC
LIMIT 100;
Critical Fields for Analysis:

block_number: Sequential identifier, use for ordering and joining
block_timestamp: UTC timestamp, essential for time-series analysis
gas_used / gas_limit: Network utilization metric (closer to 1.0 = congested)
base_fee_per_gas: Post-EIP-1559 chains only, indicates network demand
tx_count: Transaction throughput indicator

{% enddocs %}
{% docs evm_block_header_json %}
Complete block header data in JSON format. Contains all fields from the block header including:

Consensus-related fields (difficulty, nonce, mixHash)
State roots (stateRoot, transactionsRoot, receiptsRoot)
Gas and fee information
Post-merge fields (withdrawals, blobGasUsed - where applicable)

Usage: Parse this JSON when you need fields not exposed as columns, or for protocol-level analysis.
{% enddocs %}
{% docs evm_blockchain %}
The blockchain identifier for this data (e.g., 'ethereum', 'arbitrum', 'polygon'). Always lowercase.
Usage: Essential for multi-chain queries and filtering.
{% enddocs %}
{% docs evm_blocks_hash %}
The unique 32-byte Keccak-256 hash of the block header, prefixed with '0x'.
Usage:

Unique identifier for blocks across all chains
Used to verify block integrity
Reference for parent-child relationships

Example: 0x4e3a3754410177e6937ef1f84bba68ea139e8d1a2258c5f85db9f1cd715a1bdd
{% enddocs %}
{% docs evm_blocks_nonce %}
Proof-of-Work nonce value. For PoW chains, this demonstrates computational work. Post-merge Ethereum and PoS chains typically show 0x0000000000000000.
Note: Less relevant for modern PoS chains but important for historical PoW analysis.
{% enddocs %}
{% docs evm_difficulty %}
Mining difficulty at block production time.
Important:

Pre-merge: Indicates mining competition/security
Post-merge: Usually 0 for PoS chains
Some chains use different consensus mechanisms

{% enddocs %}
{% docs evm_extra_data %}
Arbitrary data included by block producer (max 32 bytes). Often contains:

Mining pool identifiers
Client version strings
Validator messages

Example: "Geth/v1.10.23-stable/linux-amd64/go1.18.5"
{% enddocs %}
{% docs evm_gas_limit %}
Maximum gas allowed for all transactions in this block. Set by miners/validators based on network rules.
Key Insights:

Network capacity indicator
Changes indicate protocol upgrades
Compare with gas_used for utilization rate

{% enddocs %}
{% docs evm_gas_used %}
Total gas consumed by all transactions in the block.
Calculation: Sum of gas used by each transaction
Usage: Network utilization = gas_used / gas_limit
{% enddocs %}
{% docs evm_network %}
Network identifier within the blockchain (e.g., 'mainnet', 'testnet'). Most production data is 'mainnet'.
{% enddocs %}
{% docs evm_parent_hash %}
Hash of the previous block (block_number - 1).
Usage:

Verify chain continuity
Detect reorganizations
Build block ancestry trees

{% enddocs %}
{% docs evm_receipts_root %}
Merkle root of all transaction receipts in the block. Used for:

Light client verification
Proof generation
State validation

{% enddocs %}
{% docs evm_sha3_uncles %}
Keccak-256 hash of uncle blocks list.
Note:

Only relevant for PoW chains
Post-merge shows empty uncles hash: 0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347

{% enddocs %}
{% docs evm_size %}
Block size in bytes. Includes:

Block header
All transactions
Uncle headers (if any)

Usage: Monitor blockchain growth rate and storage requirements
{% enddocs %}
{% docs evm_total_difficulty %}
Cumulative difficulty from genesis to this block.
Important:

PoW chains: Represents total computational work
Used for determining canonical chain
Post-merge: Frozen at merge block

{% enddocs %}
{% docs evm_tx_count %}
Number of transactions included in the block.
Insights:

0 = Empty block (still valid)
High counts = Popular applications or high activity
Useful for throughput analysis

{% enddocs %}
{% docs evm_uncle_blocks %}
Array of uncle block headers (PoW only). Uncle blocks are valid blocks that lost the race to be included in the main chain.
Key Points:

Miners receive partial rewards for uncles
Indicates network latency or mining competition
Not applicable to PoS chains

{% enddocs %}
{% docs evm_miner %}
Address that received block rewards.
Context:

PoW: Miner's address
PoS: Validator's fee recipient address
Often exchange or pool addresses

{% enddocs %}
{% docs evm_state_root %}
Merkle root of the entire blockchain state after executing this block.
Usage:

State verification
Snapshot synchronization
Archive node validation

{% enddocs %}
{% docs evm_transactions_root %}
Merkle root of all transactions in the block.
Purpose:

Efficient transaction inclusion proofs
Light client verification
Block validation

{% enddocs %}
{% docs evm_logs_bloom %}
2048-bit bloom filter containing all log addresses and topics from the block's transactions.
Usage:

Efficient log searching
Quick filtering before detailed queries
Event monitoring optimization

{% enddocs %}
{% docs evm_mix_hash %}
256-bit hash used in PoW mining process. Post-merge, contains prevRandao for randomness.
Evolution:

PoW: Mining algorithm output
PoS: RANDAO reveal for randomness

{% enddocs %}
{% docs evm_base_fee_per_gas %}
Base fee per gas unit in wei (EIP-1559 chains only).
Key Facts:

Burned (not paid to validators)
Adjusts each block based on utilization
NULL for pre-EIP-1559 blocks
Multiply by gas_used for total burned

Example Query:
sqlSELECT SUM(base_fee_per_gas * gas_used) / 1e18 AS eth_burned
FROM fact_blocks
WHERE block_timestamp >= CURRENT_DATE - 7
  AND base_fee_per_gas IS NOT NULL;
{% enddocs %}
{% docs evm_blob_gas_used %}
Gas consumed by blob transactions (EIP-4844, post-Dencun).
Context:

Used for Layer 2 data availability
Separate fee market from regular transactions
NULL for pre-Dencun blocks

{% enddocs %}
{% docs evm_excess_blob_gas %}
Excess blob gas above target, affects next block's blob base fee.
Mechanism:

Target: 3 blobs per block
Excess increases blob base fee
Self-regulating market

{% enddocs %}
{% docs evm_parent_beacon_block_root %}
Root hash of the parent beacon chain block (post-merge Ethereum).
Usage:

Links execution and consensus layers
Beacon chain verification
Only present post-merge

{% enddocs %}
{% docs evm_withdrawals %}
Array of validator withdrawals from beacon chain.
Structure: Each withdrawal contains:

index: Withdrawal sequence number
validator_index: Validator ID
address: Recipient address
amount: Withdrawn amount in Gwei

{% enddocs %}
{% docs evm_withdrawals_root %}
Merkle root of all withdrawals in the block.
Purpose:

Efficient withdrawal verification
Consensus layer integration
NULL for pre-merge blocks

{% enddocs %}