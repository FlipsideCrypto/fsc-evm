{% docs fact_blocks_table_doc %}

## What

This table contains block-level data for EVM blockchains, providing a complete record of all blocks produced on the chain. Each row represents one block with its associated metadata, transactions, and gas metrics.

## Key Use Cases

- Analyzing gas price trends and network congestion over time
- Tracking block production rates and miner/validator performance
- Monitoring network upgrades and their impact (e.g., EIP-1559, EIP-4844)
- Calculating transaction throughput and network utilization
- Identifying uncle blocks and chain reorganizations

## Important Relationships

- **Join with fact_transactions**: Use `block_number` to analyze all transactions within a block
- **Join with fact_traces**: Use `block_number` to examine internal transactions
- **Join with fact_event_logs**: Use `block_number` to find all events emitted in a block

## Commonly-used Fields

- `block_number`: Sequential identifier for blocks
- `block_timestamp`: UTC timestamp of block production
- `gas_used`: Total gas consumed by all transactions
- `gas_limit`: Maximum gas allowed in the block
- `base_fee_per_gas`: Base fee for EIP-1559 chains
- `tx_count`: Number of transactions in the block
- `miner`: Address that received block rewards

## Sample queries

```sql
-- Average gas price and utilization by hour
SELECT 
    DATE_TRUNC('hour', block_timestamp) AS hour,
    AVG(gas_used / gas_limit) AS avg_gas_utilization,
    AVG(base_fee_per_gas) AS avg_base_fee,
    COUNT(*) AS blocks_count,
    SUM(tx_count) AS total_transactions
FROM <blockchain_name>.core.fact_blocks
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
FROM <blockchain_name>.core.fact_blocks
WHERE block_timestamp >= CURRENT_DATE - 1
ORDER BY gas_used DESC
LIMIT 100;
```

{% enddocs %}

{% docs fact_blocks_block_hash %}

The unique 32-byte Keccak-256 hash of the block header, prefixed with '0x'.

Example: '0x4e3a3754410177e6937ef1f84bba68ea139e8d1a2258c5f85db9f1cd715a1bdd'

{% enddocs %}

{% docs fact_blocks_nonce %}

Proof-of-Work nonce value. For PoW chains, this demonstrates computational work. Post-merge Ethereum and PoS chains typically show 0x0000000000000000.

Example: '0x0000000000000000'

{% enddocs %}

{% docs fact_blocks_difficulty %}

Mining difficulty at block production time.

Example: 0

{% enddocs %}

{% docs fact_blocks_extra_data %}

Arbitrary data included by block producer (max 32 bytes).

Example: 'Geth/v1.10.23-stable/linux-amd64/go1.18.5'

{% enddocs %}

{% docs fact_blocks_gas_limit %}

Maximum gas allowed for all transactions in this block.

Example: 30000000

{% enddocs %}

{% docs fact_blocks_gas_used %}

Total gas consumed by all transactions in the block.

Example: 15234567

{% enddocs %}

{% docs fact_blocks_network %}

Network identifier within the blockchain (e.g., 'mainnet', 'testnet').

Example: 'mainnet'

{% enddocs %}

{% docs fact_blocks_parent_hash %}

Hash of the previous block (block_number - 1).

Example: '0x3d7a3754410177e6937ef1f84bba68ea139e8d1a2258c5f85db9f1cd715a1bee'

{% enddocs %}

{% docs fact_blocks_receipts_root %}

Merkle root of all transaction receipts in the block.

Example: '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421'

{% enddocs %}

{% docs fact_blocks_sha3_uncles %}

Keccak-256 hash of uncle blocks list.

Example: '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347'

{% enddocs %}

{% docs fact_blocks_size %}

Block size in bytes.

Example: 125432

{% enddocs %}

{% docs fact_blocks_total_difficulty %}

Cumulative difficulty from genesis to this block.

Example: 58750000000000000000000

{% enddocs %}

{% docs fact_blocks_tx_count %}

Number of transactions included in the block.

Example: 142

{% enddocs %}

{% docs fact_blocks_uncle_blocks %}

Array of uncle block headers (PoW only).

Example: []

{% enddocs %}

{% docs fact_blocks_miner %}

Address that received block rewards.

Example: '0xea674fdde714fd979de3edf0f56aa9716b898ec8'

{% enddocs %}

{% docs fact_blocks_state_root %}

Merkle root of the entire blockchain state after executing this block.

Example: '0xd7f897bbebe1f8d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d4934'

{% enddocs %}

{% docs fact_blocks_transactions_root %}

Merkle root of all transactions in the block.

Example: '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421'

{% enddocs %}

{% docs fact_blocks_logs_bloom %}

2048-bit bloom filter containing all log addresses and topics from the block's transactions.

Example: '0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'

{% enddocs %}

{% docs fact_blocks_mix_hash %}

256-bit hash used in PoW mining process. Post-merge, contains prevRandao for randomness.

Example: '0x0000000000000000000000000000000000000000000000000000000000000000'

{% enddocs %}

{% docs fact_blocks_base_fee_per_gas %}

Base fee per gas unit in wei (EIP-1559 chains only).

Example: 25000000000

{% enddocs %}

{% docs fact_blocks_blob_gas_used %}

Gas consumed by blob transactions (EIP-4844, post-Dencun).

Example: 131072

{% enddocs %}

{% docs fact_blocks_excess_blob_gas %}

Excess blob gas above target, affects next block's blob base fee.

Example: 262144

{% enddocs %}

{% docs fact_blocks_parent_beacon_block_root %}

Root hash of the parent beacon chain block (post-merge Ethereum).

Example: '0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef'

{% enddocs %}

{% docs fact_blocks_withdrawals %}

Array of validator withdrawals from beacon chain.

Example: [{"index": 1234, "validator_index": 5678, "address": "0x123...", "amount": 1000000000}]

{% enddocs %}

{% docs fact_blocks_withdrawals_root %}

Merkle root of all withdrawals in the block.

Example: '0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890'

{% enddocs %}