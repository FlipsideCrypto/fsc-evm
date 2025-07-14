{% docs dim_labels_table_doc %}

## What

The labels table is a critical dimension for blockchain analysis, providing one-to-one address identifiers that transform opaque addresses into recognizable entities. Labels are categorized into types (cex, dex, defi, etc.) and subtypes (hot_wallet, treasury, etc.) to enable sophisticated filtering and analysis.

## Key Use Cases

- Track centralized exchange flows (deposits, withdrawals, hot/cold wallet movements)
- Analyze DeFi protocol usage and cross-protocol interactions
- Monitor whale and institutional wallet activities
- Identify token contracts and their movements
- Assess labeling coverage for active addresses
- Create entity-based transaction flow analysis

## Important Relationships

- **Join with fact_transactions**: Identify exchange flows, protocol usage
- **Join with ez_token_transfers**: Track token movements by entity type
- **Join with dim_contracts**: Combine with contract metadata

## Commonly-used Fields

- `address`: Primary key for joining with transaction tables
- `label_type`: High-level category (cex, dex, defi, token, etc.)
- `label_subtype`: Specific categorization within type (hot_wallet, pool, etc.)
- `project_name`: Protocol or entity name
- `address_name`: Most specific, granular label
- `blockchain`: Network identifier for multi-chain queries

## Sample Queries

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

{% enddocs %}

{% docs dim_labels_label %}

High-level label identifying the general entity or wallet type. Often combines project_name with label_subtype.

Example: 'Binance Hot Wallet'

{% enddocs %}

{% docs dim_labels_label_address %}

The blockchain address (0x format) that this label describes. Lowercase hex string used as primary key for joining.

Example: '0x1234567890123456789012345678901234567890'

{% enddocs %}

{% docs dim_labels_address_name %}

The most specific, granular label for this address. Provides maximum detail for precise identification.

Example: 'Binance 14'

{% enddocs %}

{% docs dim_labels_blockchain %}

The blockchain network for this label. Required for multi-chain label queries.

Example: 'ethereum'

{% enddocs %}

{% docs dim_labels_creator %}

The source or creator of this label entry. Labels from verified sources may be more reliable.

Example: 'flipside'

{% enddocs %}

{% docs dim_labels_subtype %}

Specific categorization within the label type. Used for detailed filtering within broader categories.

Example: 'hot_wallet'

{% enddocs %}

{% docs dim_labels_label_type %}

High-level category describing the address's primary function. Core types include cex, dex, defi, token, nft, bridge, games, whale, institution, and l2.

Example: 'cex'

{% enddocs %}