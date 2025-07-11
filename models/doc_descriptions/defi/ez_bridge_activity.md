{% docs ez_bridge_activity_table_doc %}

## Overview
This table provides a comprehensive view of cross-chain bridge activity across EVM-compatible blockchains. It consolidates bridge-related events from multiple sources (event_logs, traces, and transfers) to create a unified dataset for analyzing cross-chain asset movements.

### Key Features
- **Multi-source aggregation**: Combines data from event_logs, traces, and transfers
- **USD valuations**: Includes token amounts converted to USD where pricing data is available
- **Cross-chain tracking**: Captures both source and destination chain information by tracking outgoing bridge activity
- **Protocol coverage**: Includes major bridge protocols with historical and current onchain activity

### Key Sources
- `core.fact_event_logs`, `core.ez_decoded_event_logs` or `core.ez_token_transfers` for event-based activity
- `core.fact_traces` or `core.ez_native_transfers` for traces-based activity
- `core.dim_contracts` or the `eth_call` RPC node method for bridge contract metadata, where available
- `price.ez_prices_hourly` for USD valuations, where available

### Sample Queries

```sql
-- Daily bridge volume by protocol
SELECT 
    DATE_TRUNC('day', block_timestamp) AS date,
    platform,
    COUNT(DISTINCT tx_hash) AS bridge_txns,
    SUM(amount_usd) AS volume_usd
FROM <blockchain_name>.defi.ez_bridge_activity
WHERE block_timestamp >= CURRENT_DATE - 30
    AND amount_usd IS NOT NULL
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;

-- Top bridge routes (source to destination chains)
SELECT 
    blockchain AS source_chain,
    destination_chain,
    platform,
    COUNT(*) AS transfer_count,
    SUM(amount_usd) AS total_volume_usd
FROM <blockchain_name>.defi.ez_bridge_activity
WHERE block_timestamp >= CURRENT_DATE - 7
    AND destination_chain IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 5 DESC
LIMIT 20;

-- User bridge activity analysis
SELECT 
    sender,
    COUNT(DISTINCT DATE_TRUNC('day', block_timestamp)) AS active_days,
    COUNT(DISTINCT platform) AS protocols_used,
    COUNT(DISTINCT destination_chain) AS chains_bridged_to,
    SUM(amount_usd) AS total_bridged_usd
FROM <blockchain_name>.defi.ez_bridge_activity
WHERE block_timestamp >= CURRENT_DATE - 30
    AND amount_usd > 100  -- Filter small transfers
GROUP BY 1
HAVING COUNT(*) > 5  -- Active bridgers
ORDER BY 5 DESC
LIMIT 100;

-- Token flow analysis
SELECT 
    token_symbol,
    token_address,
    blockchain AS source_chain,
    destination_chain,
    COUNT(*) AS bridge_count,
    SUM(amount) AS total_amount,
    AVG(amount_usd) AS avg_transfer_usd
FROM <blockchain_name>.defi.ez_bridge_activity
WHERE block_timestamp >= CURRENT_DATE - 7
    AND token_symbol IS NOT NULL
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) > 10
ORDER BY 5 DESC;

-- Bridge protocol comparison
WITH protocol_stats AS (
    SELECT 
        platform,
        COUNT(DISTINCT sender) AS unique_users,
        COUNT(*) AS total_transfers,
        AVG(amount_usd) AS avg_transfer_size,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount_usd) AS median_transfer_size,
        SUM(amount_usd) AS total_volume
    FROM <blockchain_name>.defi.ez_bridge_activity
    WHERE block_timestamp >= CURRENT_DATE - 30
        AND amount_usd IS NOT NULL
    GROUP BY 1
)
SELECT *
FROM protocol_stats
ORDER BY total_volume DESC;
```

### Critical Usage Notes
- **Coverage limitations**: This table only includes protocols with decoded bridge events. Some bridges may not be covered
- **USD values**: `amount_usd` may be NULL for tokens without price data or during high volatility periods
- **Destination data**: Non-EVM destination chains may not have decoded receiver addresses. Addresses may exist in their raw, non-transformed format
- **Latency**: Bridge completions on destination chains are not tracked in this table. This table tracks outgoing activity only
- **Performance tip**: Always filter by `block_timestamp` for large queries

### Data Quality Considerations
- In some cases, Bridge events may be identified through pattern matching and may have false positives
- Some protocols may have partial coverage during initial integration periods
- Cross-chain message passing (non-token transfers) is not included
- Wrapped token representations may vary between source and destination chains
- Destination chain names are generally standardized for consistency purposes and ease of use, via a manual process, in lowercase and snake_case. Chain names with slight discrepencies may exist, and Flipside will standardize accordingly (e.g. mantle vs mantle network)

{% enddocs %}

{% docs ez_bridge_activity_platform %}

## platform
The protocol or application facilitating the cross-chain bridge transfer.

### Details
- **Type**: `VARCHAR`
- **Common values**: `'across'`, `'stargate'`, `'hop'`, `'synapse'`, `'cbridge'`, `'multichain'`, `'wormhole'`
- **NULL handling**: Should not be NULL; indicates unidentified bridge protocol

### Usage Examples
```sql
-- Filter for specific bridge protocol
SELECT * 
FROM <blockchain_name>.defi.ez_bridge_activity 
WHERE platform = 'stargate' 
  AND block_timestamp >= CURRENT_DATE - 1
LIMIT 100;

-- Compare protocol market share
SELECT platform, 
       COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS market_share_pct
FROM <blockchain_name>.defi.ez_bridge_activity
WHERE block_timestamp >= CURRENT_DATE - 7
GROUP BY platform;
```

### Notes
- Platform names are lowercase and standardized
- New platforms are added as they are integrated
- Some platforms may have multiple bridge contracts

{% enddocs %}

{% docs ez_bridge_activity_origin_from_address %}

## origin_from_address
The address that initiated the bridge transaction, typically representing the end user.

### Details
- **Type**: `VARCHAR(42)` - Ethereum address format
- **Format**: `0x` prefixed, 40 hexadecimal characters
- **Common patterns**: Usually an EOA, occasionally a smart contract wallet or aggregator

### Usage Examples
```sql
-- Find multi-chain users
SELECT origin_from_address, 
       COUNT(DISTINCT blockchain) AS source_chains_used,
       COUNT(DISTINCT destination_chain) AS dest_chains_used
FROM <blockchain_name>.defi.ez_bridge_activity
GROUP BY origin_from_address
HAVING COUNT(DISTINCT blockchain) >= 2;

-- Analyze user behavior patterns
SELECT origin_from_address,
       AVG(HOURS_BETWEEN(block_timestamp, LAG(block_timestamp) 
           OVER (PARTITION BY origin_from_address ORDER BY block_timestamp))) AS avg_hours_between_bridges
FROM <blockchain_name>.defi.ez_bridge_activity
WHERE origin_from_address IN (SELECT origin_from_address FROM <blockchain_name>.defi.ez_bridge_activity 
                      GROUP BY 1 HAVING COUNT(*) > 10);
```

### Relationship Notes
- May differ from `sender` when using contract wallets or aggregators
- May link to `core.dim_labels` for address identification
- Can be used to track user journey across chains

{% enddocs %}

{% docs ez_bridge_activity_sender %}

## sender
The address that directly sent tokens to the bridge contract.

### Details
- **Type**: `VARCHAR(42)`
- **Relationship**: May equal `origin_from_address` for direct interactions
- **Common differences**: Differs when using routers, aggregators, or smart wallets

### Usage Examples
```sql
-- Identify aggregator usage
SELECT COUNT(*) AS aggregator_txns
FROM <blockchain_name>.defi.ez_bridge_activity
WHERE sender != origin_from_address
  AND sender IN (SELECT address FROM core.dim_contracts 
                 WHERE contract_name LIKE '%aggregator%');

-- Direct vs indirect bridge usage
SELECT 
    CASE WHEN sender = origin_from_address THEN 'Direct' ELSE 'Indirect' END AS interaction_type,
    COUNT(*) AS transfer_count,
    AVG(amount_usd) AS avg_transfer_size
FROM <blockchain_name>.defi.ez_bridge_activity
WHERE amount_usd IS NOT NULL
GROUP BY 1;
```

{% enddocs %}

{% docs ez_bridge_activity_receiver %}

## receiver
The address designated to receive tokens on the destination chain (or on the source chain, for intermediate steps).

### Details
- **Type**: `VARCHAR(42)`
- **Usage**: May represents the true receiver of the tokens or intermediate custody or escrow addresses
- **NULL handling**: May be NULL for direct burn-and-mint bridges

### Usage Examples
```sql
-- Analyze self vs third-party transfers
SELECT 
    CASE WHEN sender = receiver THEN 'Self' ELSE 'Third-party' END AS transfer_type,
    COUNT(*) AS count,
    AVG(amount_usd) AS avg_amount_usd
FROM <blockchain_name>.defi.ez_bridge_activity
WHERE receiver IS NOT NULL
GROUP BY 1;
```

{% enddocs %}

{% docs ez_bridge_activity_destination_chain_receiver %}

## destination_chain_receiver
The final recipient address on the destination blockchain.

### Details
- **Type**: `VARCHAR`
- **Format variations**: 
  - EVM chains: Standard `0x` addresses
  - Non-EVM chains: Encoded/decoded based on destination chain format
  - Examples: Base58 for Solana, Bech32 for Cosmos chains
  - Proper decoding/encoding not guaranteed. Addresses may exist in their raw, non-transformed format

### Usage Examples
```sql
-- Identify address format by destination
SELECT 
    destination_chain,
    LENGTH(destination_chain_receiver) AS addr_length,
    LEFT(destination_chain_receiver, 4) AS addr_prefix,
    COUNT(DISTINCT destination_chain_receiver) AS unique_addresses
FROM <blockchain_name>.defi.ez_bridge_activity
WHERE destination_chain_receiver IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1, 4 DESC;
```

### Important Notes
- Address format validation depends on destination chain
- Conversion errors may result in NULL values
- Some bridges use intermediate addresses that differ from final recipient

{% enddocs %}

{% docs ez_bridge_activity_destination_chain %}

## destination_chain
The target blockchain network for the bridged assets.

### Details
- **Type**: `VARCHAR`
- **Common values**: `'ethereum'`, `'polygon'`, `'arbitrum'`, `'optimism'`, `'avalanche'`, `'bsc'`, `'solana'`, `'cosmos'`
- **Case**: Lowercase standardized names

### Usage Examples
```sql
-- Popular bridge routes
SELECT 
    CONCAT(blockchain, ' -> ', destination_chain) AS route,
    COUNT(*) AS transfer_count,
    SUM(amount_usd) AS volume_usd
FROM <blockchain_name>.defi.ez_bridge_activity
WHERE destination_chain IS NOT NULL
  AND amount_usd IS NOT NULL
GROUP BY 1
ORDER BY 3 DESC
LIMIT 20;

-- Chain ecosystem analysis
SELECT 
    destination_chain,
    COUNT(DISTINCT sender) AS unique_bridgers,
    COUNT(DISTINCT token_address) AS unique_tokens,
    SUM(amount_usd) AS total_inflow_usd
FROM <blockchain_name>.defi.ez_bridge_activity
WHERE block_timestamp >= CURRENT_DATE - 30
GROUP BY 1
ORDER BY 4 DESC;
```

{% enddocs %}

{% docs ez_bridge_activity_destination_chain_id %}

## destination_chain_id
The numeric identifier for the destination blockchain.

### Details
- **Type**: `INTEGER`
- **Standards**: 
  - EVM chains: EIP-155 chain IDs
  - Non-EVM: Protocol-specific identifiers
- **Common values**: `1` (Ethereum), `137` (Polygon), `42161` (Arbitrum), `10` (Optimism)

### Usage Examples
```sql
-- Map chain IDs to names
SELECT DISTINCT 
    destination_chain,
    destination_chain_id
FROM <blockchain_name>.defi.ez_bridge_activity
WHERE destination_chain_id IS NOT NULL
ORDER BY destination_chain_id;

-- Filter by L2s
SELECT * 
FROM <blockchain_name>.defi.ez_bridge_activity
WHERE destination_chain_id IN (42161, 10, 8453) -- Arbitrum, Optimism, Base
  AND block_timestamp >= CURRENT_DATE - 1
LIMIT 100;
```

{% enddocs %}

{% docs ez_bridge_activity_bridge_address %}

## bridge_address
The smart contract address handling the bridge operation.

### Details
- **Type**: `VARCHAR(42)`
- **Purpose**: The entry point contract for bridge deposits
- **Variations**: Each protocol may have multiple bridge addresses for different tokens or routes

### Usage Examples
```sql
-- Most active bridge contracts
SELECT 
    bridge_address,
    platform,
    COUNT(*) AS usage_count,
    COUNT(DISTINCT sender) AS unique_users
FROM <blockchain_name>.defi.ez_bridge_activity
WHERE block_timestamp >= CURRENT_DATE - 7
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 20;

-- Bridge contract token support
SELECT 
    bridge_address,
    COUNT(DISTINCT token_address) AS supported_tokens,
    ARRAY_AGG(DISTINCT token_symbol) AS token_list
FROM <blockchain_name>.defi.ez_bridge_activity
GROUP BY 1;
```

### Performance Notes
- Index on `bridge_address` for efficient filtering
- Join with `core.dim_contracts` for additional contract metadata

{% enddocs %}

{% docs ez_bridge_activity_token_address %}

## token_address
The contract address of the token being bridged.

### Details
- **Type**: `VARCHAR(42)`
- **NULL cases**: While native token transfers do not have an associated token address, this value may be represented by the Wrapped Native version of the asset, for ease of use and for prices (ETH vs WETH, MATIC vs WMATIC etc.)
- **Standards**: ERC-20 tokens on source chain

### Usage Examples
```sql
-- Most bridged tokens
SELECT 
    token_address,
    token_symbol,
    COUNT(*) AS bridge_count,
    SUM(amount_usd) AS total_volume_usd,
    COUNT(DISTINCT destination_chain) AS destination_count
FROM <blockchain_name>.defi.ez_bridge_activity
WHERE amount_usd IS NOT NULL
GROUP BY 1, 2
ORDER BY 4 DESC
LIMIT 50;
```

{% enddocs %}

{% docs ez_bridge_activity_token_symbol %}

## token_symbol
The symbol identifier for the bridged token.

### Details
- **Type**: `VARCHAR`
- **Common values**: `'USDC'`, `'USDT'`, `'WETH'`, `'DAI'`, `'WBTC'`
- **NULL handling**: May be NULL for unverified or new tokens

### Usage Examples
```sql
-- Stablecoin bridge flows
SELECT 
    token_symbol,
    blockchain AS source_chain,
    destination_chain,
    SUM(amount) AS total_amount,
    COUNT(*) AS transfer_count
FROM <blockchain_name>.defi.ez_bridge_activity
WHERE token_symbol IN ('USDC', 'USDT', 'DAI', 'BUSD')
  AND block_timestamp >= CURRENT_DATE - 7
GROUP BY 1, 2, 3
ORDER BY 4 DESC;

-- Symbol standardization check
SELECT 
    token_symbol,
    COUNT(DISTINCT token_address) AS address_count,
    ARRAY_AGG(DISTINCT token_address) AS addresses
FROM <blockchain_name>.defi.ez_bridge_activity
WHERE token_symbol IS NOT NULL
GROUP BY 1
HAVING COUNT(DISTINCT token_address) > 1
ORDER BY 2 DESC;
```

{% enddocs %}

{% docs ez_bridge_activity_amount_unadj %}

## amount_unadj
The raw token amount without decimal adjustment.

### Details
- **Type**: `NUMERIC`
- **Usage**: Raw amount as emitted in events/traces
- **Precision**: Full precision not guaranteed

### Usage Examples
```sql
-- Calculate decimal places for tokens
SELECT 
    token_symbol,
    token_address,
    LOG(10, AVG(amount_unadj::FLOAT / NULLIF(amount::FLOAT, 0))) AS implied_decimals
FROM <blockchain_name>.defi.ez_bridge_activity
WHERE amount IS NOT NULL 
  AND amount > 0
  AND amount_unadj IS NOT NULL
GROUP BY 1, 2
HAVING COUNT(*) > 100;
```

{% enddocs %}

{% docs ez_bridge_activity_amount %}

## amount
The decimal-adjusted amount of tokens bridged.

### Details
- **Type**: `NUMERIC`
- **Calculation**: `amount_unadj / POW(10, decimals)`
- **NULL cases**: NULL when decimal information unavailable

### Usage Examples
```sql
-- Large transfers (whales)
SELECT 
    tx_hash,
    sender,
    token_symbol,
    amount,
    amount_usd,
    destination_chain
FROM <blockchain_name>.defi.ez_bridge_activity
WHERE amount_usd > 1000000
  AND block_timestamp >= CURRENT_DATE - 30
ORDER BY amount_usd DESC;

-- Average transfer sizes by token
SELECT 
    token_symbol,
    AVG(amount) AS avg_amount,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) AS median_amount,
    MAX(amount) AS max_amount
FROM <blockchain_name>.defi.ez_bridge_activity
WHERE amount IS NOT NULL
  AND token_symbol IS NOT NULL
GROUP BY 1
HAVING COUNT(*) > 1000
ORDER BY 2 DESC;
```

{% enddocs %}

{% docs ez_bridge_activity_amount_usd %}

## amount_usd
The hourly close USD value of bridged tokens at the time of the transaction.

### Details
- **Type**: `NUMERIC`
- **Source**: Calculated using hourly price data
- **NULL cases**: 
  - No price data available for token
  - Pricing service downtime
  - New/untracked tokens

### Usage Examples
```sql
-- Daily bridge volume
SELECT 
    DATE_TRUNC('day', block_timestamp) AS date,
    SUM(amount_usd) AS daily_volume_usd,
    COUNT(*) AS transfer_count,
    AVG(amount_usd) AS avg_transfer_size
FROM <blockchain_name>.defi.ez_bridge_activity
WHERE amount_usd IS NOT NULL
  AND block_timestamp >= CURRENT_DATE - 30
GROUP BY 1
ORDER BY 1 DESC;

-- Price data coverage analysis
SELECT 
    token_symbol,
    COUNT(*) AS total_transfers,
    COUNT(amount_usd) AS priced_transfers,
    COUNT(amount_usd) * 100.0 / COUNT(*) AS coverage_pct
FROM <blockchain_name>.defi.ez_bridge_activity
WHERE token_symbol IS NOT NULL
GROUP BY 1
HAVING COUNT(*) > 100
ORDER BY 4 ASC;

-- High-value transfer monitoring
SELECT 
    block_timestamp,
    tx_hash,
    platform,
    sender,
    token_symbol,
    amount,
    amount_usd,
    destination_chain
FROM <blockchain_name>.defi.ez_bridge_activity
WHERE amount_usd > 500000
  AND block_timestamp >= CURRENT_DATE - 1
ORDER BY amount_usd DESC;
```

### Performance Tips
- Filter by `amount_usd IS NOT NULL` to exclude unpriced transfers, however be aware, this may result in understated volumes.
- Use `block_timestamp` filters before aggregating large USD volumes
- Consider using materialized views for frequently queried USD aggregations

{% enddocs %}

{% docs ez_bridge_activity_token_is_verified %}

## token_is_verified
Whether the token is verified by the Flipside team.

### Details
- **Type**: `BOOLEAN`
- **Usage**: Whether the token is verified.

{% enddocs %}