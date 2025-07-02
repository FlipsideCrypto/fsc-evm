{% docs dim_labels_table_doc %}

## Table: dim_labels (Extended)

The labels table is a critical dimension for blockchain analysis, providing one-to-one address identifiers that transform opaque addresses into recognizable entities. Labels are categorized into types (cex, dex, defi, etc.) and subtypes (hot_wallet, treasury, etc.) to enable sophisticated filtering and analysis.

### Label Sources:
1. **Automatic Labeling**:
   - Contract deployment tracking
   - Behavioral pattern recognition
   - Protocol API integrations
   - Exchange deposit address detection

2. **Manual Curation**:
   - Protocol team submissions
   - Community contributions
   - Trending address identification
   - Exchange wallet verification

3. **Community Contributions**:
   - [Web Label Submission Tool](https://science.flipsidecrypto.xyz/add-a-label/)
   - [NEAR On-Chain Submission](https://near.social/lord1.near/widget/Form)
   - Reviews by Flipside labels team

### Label Categories:

| Type | Description | Common Subtypes |
|------|-------------|-----------------|
| cex | Centralized Exchanges | hot_wallet, cold_wallet, deposit |
| dex | Decentralized Exchanges | pool, router, factory |
| defi | DeFi Protocols | lending_pool, vault, staking |
| bridge | Cross-chain Bridges | escrow, relayer |
| nft | NFT Platforms | marketplace, collection |
| token | Token Contracts | token_contract, lptoken |
| games | Gaming/GameFi | treasury, rewards_pool |
| l2 | Layer 2 Solutions | sequencer, bridge |

### Important Relationships:
- **Join with fact_transactions**: Identify exchange flows, protocol usage
- **Join with ez_token_transfers**: Track token movements by entity type
- **Join with dim_contracts**: Combine with contract metadata

### Sample Queries:

**Exchange Flow Analysis**
```sql
-- Track CEX inflows and outflows
WITH cex_addresses AS (
    SELECT DISTINCT address
    FROM <blockchain_name>.core.dim_labels
    WHERE label_type = 'cex'
        AND label_subtype IN ('hot_wallet', 'deposit')
)
SELECT 
    DATE_TRUNC('day', t.block_timestamp) AS day,
    CASE 
        WHEN t.to_address IN (SELECT address FROM cex_addresses) THEN 'CEX Inflow'
        WHEN t.from_address IN (SELECT address FROM cex_addresses) THEN 'CEX Outflow'
    END AS flow_direction,
    COUNT(*) AS transaction_count,
    SUM(t.value) AS total_native_value,
    COUNT(DISTINCT t.from_address) AS unique_users
FROM <blockchain_name>.fact.fact_transactions t
WHERE (t.to_address IN (SELECT address FROM cex_addresses)
       OR t.from_address IN (SELECT address FROM cex_addresses))
    AND t.block_timestamp >= CURRENT_DATE - 30
    AND t.value > 0
GROUP BY 1, 2
ORDER BY 1 DESC, 2;
```

**Protocol Usage Ranking**
```sql
-- Top DeFi protocols by unique users
SELECT 
    l.project_name,
    l.label_type,
    COUNT(DISTINCT t.from_address) AS unique_users,
    COUNT(*) AS total_interactions,
    SUM(t.tx_fee) AS total_fees_paid
FROM <blockchain_name>.fact.fact_transactions t
JOIN <blockchain_name>.core.dim_labels l ON t.to_address = l.address
WHERE l.label_type IN ('defi', 'dex')
    AND t.block_timestamp >= CURRENT_DATE - 7
    AND t.tx_status = 'SUCCESS'
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 20;
```

**Whale Wallet Tracking**
```sql
-- Monitor large holder activities
WITH whale_activity AS (
    SELECT 
        l.address,
        l.address_name,
        l.label_type,
        COUNT(*) AS tx_count,
        SUM(CASE WHEN t.from_address = l.address THEN 1 ELSE 0 END) AS outgoing_tx,
        SUM(CASE WHEN t.to_address = l.address THEN 1 ELSE 0 END) AS incoming_tx,
        SUM(CASE WHEN t.from_address = l.address THEN t.value ELSE 0 END) AS value_sent,
        SUM(CASE WHEN t.to_address = l.address THEN t.value ELSE 0 END) AS value_received
    FROM <blockchain_name>.core.dim_labels l
    JOIN <blockchain_name>.fact.fact_transactions t 
        ON l.address IN (t.from_address, t.to_address)
    WHERE l.label_type IN ('whale', 'institution', 'fund')
        AND t.block_timestamp >= CURRENT_DATE - 1
    GROUP BY 1, 2, 3
)
SELECT 
    *,
    value_received - value_sent AS net_flow
FROM whale_activity
WHERE tx_count > 10
ORDER BY ABS(net_flow) DESC;
```

**Cross-Protocol Interactions**
```sql
-- Find addresses interacting with multiple protocols
WITH user_protocols AS (
    SELECT 
        t.from_address AS user_address,
        l.project_name,
        l.label_type,
        COUNT(*) AS interactions
    FROM <blockchain_name>.fact.fact_transactions t
    JOIN <blockchain_name>.core.dim_labels l ON t.to_address = l.address
    WHERE l.label_type IN ('defi', 'dex', 'nft')
        AND t.block_timestamp >= CURRENT_DATE - 30
    GROUP BY 1, 2, 3
)
SELECT 
    user_address,
    COUNT(DISTINCT project_name) AS protocols_used,
    SUM(interactions) AS total_interactions,
    ARRAY_AGG(DISTINCT project_name) AS protocol_list
FROM user_protocols
GROUP BY 1
HAVING COUNT(DISTINCT project_name) >= 5
ORDER BY 2 DESC
LIMIT 100;
```

**Label Coverage Analysis**
```sql
-- Assess labeling coverage for active addresses
WITH active_addresses AS (
    SELECT DISTINCT address, address_count
    FROM (
        SELECT to_address AS address, COUNT(*) AS address_count
        FROM <blockchain_name>.fact.fact_transactions
        WHERE block_timestamp >= CURRENT_DATE - 7
        GROUP BY 1
        HAVING COUNT(*) > 100
    )
)
SELECT 
    CASE WHEN l.address IS NOT NULL THEN 'Labeled' ELSE 'Unlabeled' END AS status,
    COUNT(*) AS address_count,
    SUM(a.address_count) AS total_transactions
FROM active_addresses a
LEFT JOIN <blockchain_name>.core.dim_labels l ON a.address = l.address
GROUP BY 1;
```

### Best Practices:
- **Use Latest Labels**: Labels are continuously updated
- **Verify Label Types**: Confirm label_type matches your analysis needs
- **Handle Multiple Labels**: Some addresses may have evolved purposes
- **Cross-Reference**: Validate critical labels with on-chain behavior

{% enddocs %}

{% docs dim_labels_label %}

High-level label identifying the general entity or wallet type.

**Common Values**:
- "Binance Hot Wallet"
- "Uniswap V3 Router"
- "USDC Token Contract"
- "Vitalik Buterin"

**Relationship**: Often combines project_name with label_subtype

{% enddocs %}

{% docs dim_labels_label_address %}

The blockchain address (0x format) that this label describes.

**Format**: Lowercase hex string (0x + 40 characters)
**Primary Key**: Unique identifier for joining

**Join Pattern**:
```sql
-- Standard label join
LEFT JOIN <blockchain_name>.core.dim_labels l 
ON t.to_address = l.address
```

{% enddocs %}

{% docs dim_labels_address_name %}

The most specific, granular label for this address.

**Examples**:
- "Binance 14"
- "Uniswap V3: USDC-ETH 0.05%"
- "Circle: USDC Treasury"

**Usage**: Provides maximum detail for precise identification

**Display Pattern**:
```sql
-- Show most specific label with fallbacks
COALESCE(
    l.address_name,
    l.label,
    l.project_name || ': ' || l.label_subtype
) AS display_name
```

{% enddocs %}

{% docs dim_labels_blockchain %}

The blockchain network for this label.

**Values**: ethereum, polygon, avalanche, etc.
**Usage**: Required for multi-chain label queries

**Multi-Chain Query**:
```sql
-- Same entity across chains
SELECT 
    blockchain,
    address,
    address_name
FROM <blockchain_name>.core.dim_labels
WHERE project_name = 'Circle'
ORDER BY blockchain;
```

{% enddocs %}

{% docs dim_labels_creator %}

The source or creator of this label entry.

**Common Values**:
- "flipside"
- "community"
- "protocol_team"
- Specific usernames for community submissions

**Quality Indicator**: Labels from verified sources may be more reliable

{% enddocs %}

{% docs dim_labels_subtype %}

Specific categorization within the label type.

**Examples by Type**:
- **cex**: hot_wallet, cold_wallet, deposit
- **dex**: pool, router, factory, lptoken
- **defi**: lending_pool, vault, staking, treasury
- **token**: token_contract, lptoken
- **bridge**: escrow, relayer

**Filtering Pattern**:
```sql
-- Find all DEX liquidity pools
WHERE label_type = 'dex' 
  AND label_subtype = 'pool'
```

{% enddocs %}

{% docs dim_labels_label_type %}

High-level category describing the address's primary function.

**Core Types**:
- `cex`: Centralized exchanges
- `dex`: Decentralized exchanges  
- `defi`: DeFi protocols
- `token`: Token contracts
- `nft`: NFT platforms and collections
- `bridge`: Cross-chain bridges
- `games`: Gaming and GameFi
- `whale`: Large holders
- `institution`: Institutional wallets
- `l2`: Layer 2 infrastructure

**Analysis Usage**:
```sql
-- Ecosystem breakdown
SELECT 
    label_type,
    COUNT(DISTINCT address) AS address_count,
    COUNT(DISTINCT project_name) AS project_count
FROM <blockchain_name>.core.dim_labels
GROUP BY 1
ORDER BY 2 DESC;
```

{% enddocs %}