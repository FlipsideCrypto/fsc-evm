{% docs ez_bridge_activity_table_doc %}

## What

This table provides a comprehensive view of cross-chain bridge activity across EVM-compatible blockchains. It consolidates bridge-related events from multiple sources (event_logs, traces, and transfers) to create a unified dataset for analyzing cross-chain asset movements.

## Key Use Cases

- Tracking cross-chain asset flows and bridge volumes
- Analyzing user bridging behavior and patterns
- Comparing bridge protocol market share and performance
- Monitoring token distribution across multiple chains
- Identifying popular bridge routes and corridors

## Important Relationships

- **Join with core.fact_event_logs**: Use `tx_hash` for raw event details
- **Join with core.dim_contracts**: Use `bridge_address` or `token_address` for contract metadata
- **Join with price.ez_prices_hourly**: For additional price validation
- **Join with core.dim_labels**: Use sender addresses for entity identification

## Commonly-used Fields

- `platform`: Bridge protocol name
- `sender`: Address sending tokens to bridge
- `destination_chain`: Target blockchain for assets
- `token_address`: Token being bridged
- `amount`: Decimal-adjusted token amount
- `amount_usd`: USD value at transaction time
- `block_timestamp`: When bridge transaction occurred

## Sample queries

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

{% enddocs %}

{% docs ez_bridge_activity_platform %}

The protocol or application facilitating the cross-chain bridge transfer.

Example: 'stargate'

{% enddocs %}

{% docs ez_bridge_activity_origin_from_address %}

The address that initiated the bridge transaction, typically representing the end user.

Example: '0x1234567890123456789012345678901234567890'

{% enddocs %}

{% docs ez_bridge_activity_sender %}

The address that directly sent tokens to the bridge contract.

Example: '0xabcdefabcdefabcdefabcdefabcdefabcdefabcd'

{% enddocs %}

{% docs ez_bridge_activity_receiver %}

The address designated to receive tokens on the destination chain (or on the source chain, for intermediate steps).

Example: '0x9876543210987654321098765432109876543210'

{% enddocs %}

{% docs ez_bridge_activity_destination_chain_receiver %}

The final recipient address on the destination blockchain.

Example: '0xfedcbafedcbafedcbafedcbafedcbafedcbafed'

{% enddocs %}

{% docs ez_bridge_activity_destination_chain %}

The target blockchain network for the bridged assets.

Example: 'arbitrum'

{% enddocs %}

{% docs ez_bridge_activity_destination_chain_id %}

The numeric identifier for the destination blockchain.

Example: 42161

{% enddocs %}

{% docs ez_bridge_activity_bridge_address %}

The smart contract address handling the bridge operation.

Example: '0x296f55f8fb28e498b858d0bcda06d955b2cb3f97'

{% enddocs %}

{% docs ez_bridge_activity_token_address %}

The contract address of the token being bridged.

Example: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'

{% enddocs %}

{% docs ez_bridge_activity_token_symbol %}

The symbol identifier for the bridged token.

Example: 'USDC'

{% enddocs %}

{% docs ez_bridge_activity_amount_unadj %}

The raw token amount without decimal adjustment.

Example: 1000000

{% enddocs %}

{% docs ez_bridge_activity_amount %}

The decimal-adjusted amount of tokens bridged.

Example: 1.0

{% enddocs %}

{% docs ez_bridge_activity_amount_usd %}

The hourly close USD value of bridged tokens at the time of the transaction.

Example: 1000.50

{% enddocs %}

{% docs ez_bridge_activity_token_is_verified %}

Whether the token is verified by the Flipside team.

Example: true

{% enddocs %}