{% docs ez_nft_sales_table_doc %}

## Overview
This table provides a comprehensive view of NFT (Non-Fungible Token) sales across all major marketplaces and platforms on EVM blockchains. It captures both direct marketplace sales and aggregator-routed transactions, enabling analysis of NFT market dynamics, collection performance, and trading patterns.

### Key Features
- **Multi-marketplace coverage**: Includes OpenSea, Blur, LooksRare, X2Y2, and other major platforms
- **Aggregator support**: Tracks sales routed through aggregators like Gem and Genie
- **Fee breakdown**: Separates platform fees and creator royalties
- **USD valuations**: Converts all sale prices to USD using historical rates
- **Token standards**: Supports ERC-721, ERC-1155, and other NFT standards

### Important Relationships
- Links to `core.fact_event_logs` via `tx_hash` and `event_index`
- Joins with `core.dim_contracts` for collection name
- References `price.ez_prices_hourly` for currency conversions
- Connects to token transfer tables for ownership tracking

### Sample Queries

```sql
-- Daily NFT market volume by platform
SELECT 
    DATE_TRUNC('day', block_timestamp) AS date,
    platform_name,
    COUNT(*) as sales_count,
    COUNT(DISTINCT tx_hash) AS unique_sales_transaction_count,
    COUNT(DISTINCT buyer_address) AS unique_buyers,
    COUNT(DISTINCT contract_address) AS collections_traded,
    SUM(price_usd) AS total_volume_usd,
    AVG(price_usd) AS avg_sale_price_usd
FROM <blockchain_name>.nft.ez_nft_sales
WHERE block_timestamp >= CURRENT_DATE - 30
    AND price_usd IS NOT NULL
GROUP BY 1, 2
ORDER BY 1 DESC, 7 DESC;

-- Top selling NFT collections
SELECT 
    contract_address,
    name AS collection_name,
    COUNT(*) AS sales_count,
    COUNT(DISTINCT token_id) AS unique_tokens_sold,
    COUNT(DISTINCT buyer_address) AS unique_buyers,
    SUM(price_usd) AS total_volume_usd,
    AVG(price_usd) AS avg_price_usd,
    MAX(price_usd) AS highest_sale_usd
FROM <blockchain_name>.nft.ez_nft_sales
WHERE block_timestamp >= CURRENT_DATE - 7
    AND price_usd IS NOT NULL
GROUP BY 1, 2
ORDER BY 6 DESC
LIMIT 50;

-- Marketplace competition analysis
WITH platform_metrics AS (
    SELECT 
        platform_name,
        COUNT(*) AS total_sales,
        SUM(price_usd) AS total_volume_usd,
        COUNT(DISTINCT buyer_address) AS unique_buyers,
        COUNT(DISTINCT seller_address) AS unique_sellers,
        AVG(platform_fee / NULLIF(price, 0) * 100) AS avg_platform_fee_pct,
        SUM(platform_fee_usd) AS total_platform_revenue_usd
    FROM <blockchain_name>.nft.ez_nft_sales
    WHERE block_timestamp >= CURRENT_DATE - 30
        AND price_usd IS NOT NULL
    GROUP BY 1
)
SELECT 
    platform_name,
    total_sales,
    total_volume_usd,
    total_volume_usd * 100.0 / SUM(total_volume_usd) OVER () AS market_share_pct,
    unique_buyers,
    unique_sellers,
    avg_platform_fee_pct,
    total_platform_revenue_usd
FROM platform_metrics
ORDER BY total_volume_usd DESC;

-- Whale activity tracking
WITH buyer_stats AS (
    SELECT 
        buyer_address,
        COUNT(*) AS purchases,
        COUNT(DISTINCT contract_address) AS unique_collections,
        SUM(price_usd) AS total_spent_usd,
        AVG(price_usd) AS avg_purchase_price,
        MAX(price_usd) AS highest_purchase
    FROM <blockchain_name>.nft.ez_nft_sales
    WHERE block_timestamp >= CURRENT_DATE - 30
        AND price_usd IS NOT NULL
    GROUP BY 1
)
SELECT 
    CASE 
        WHEN total_spent_usd < 1000 THEN '< $1K'
        WHEN total_spent_usd < 10000 THEN '$1K - $10K'
        WHEN total_spent_usd < 100000 THEN '$10K - $100K'
        WHEN total_spent_usd < 1000000 THEN '$100K - $1M'
        ELSE '> $1M'
    END AS buyer_tier,
    COUNT(*) AS buyer_count,
    SUM(purchases) AS total_purchases,
    AVG(unique_collections) AS avg_collections_per_buyer,
    SUM(total_spent_usd) AS tier_total_spent
FROM buyer_stats
GROUP BY 1
ORDER BY MIN(total_spent_usd);

-- Creator royalty analysis
SELECT 
    name AS collection_name,
    contract_address,
    COUNT(*) AS sales_with_royalties,
    SUM(creator_fee) AS total_creator_fees,
    SUM(creator_fee_usd) AS total_creator_fees_usd,
    AVG(creator_fee / NULLIF(price, 0) * 100) AS avg_royalty_pct,
    SUM(creator_fee_usd) / NULLIF(SUM(price_usd), 0) * 100 AS effective_royalty_rate
FROM <blockchain_name>.nft.ez_nft_sales
WHERE creator_fee > 0
    AND price > 0
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY 1, 2
HAVING COUNT(*) > 10
ORDER BY total_creator_fees_usd DESC
LIMIT 100;
```

### Critical Usage Notes
- **Aggregator routing**: Sales through aggregators show the aggregator in `aggregator_name`
- **Fee handling**: Not all sales include creator fees (depends on marketplace enforcement)
- **Price accuracy**: `price_usd` may be NULL for tokens without USD conversion rates
- **Event types**: Filter by `event_type` for specific sale types (sale, bid_won, etc.) Regardless of event types, all transactions should be considered as sales. 
- **Performance tip**: Always filter by `block_timestamp` for large queries

### Data Quality Considerations
- Some marketplaces may have delayed or missing fee information
- Wash trading detection requires additional analysis
- Bundle sales may show as single transactions with multiple token IDs
- Private sales may not appear if conducted outside tracked platforms

{% enddocs %}

{% docs ez_nft_sales_event_type %}

## Event Type
The specific type of NFT transaction that occurred.

### Details
- **Type**: `VARCHAR`
- **Common values**: `'sale'`, `'bid_won'`, `'redeem'`, `'mint'`.  `Sale` represents a direct purchase from a listing. `Bid_won` represents the NFT seller accepted a bid on their listed NFT. `Redeem` and `Mint` represent NFT sales from minting or redeeming from ERC-20 pools that represent pools of NFTs. These mechanics are not common and are done via a handful of platforms such as NFTx. 
- **Purpose**: Distinguishes between different sale mechanisms. For regular analysis, it is not necessary to filter by event_type 

### Usage Examples
```sql
-- Sales by event type
SELECT 
    event_type,
    COUNT(*) AS transaction_count,
    SUM(price_usd) AS total_volume_usd,
    AVG(price_usd) AS avg_price_usd,
    COUNT(DISTINCT platform_name) AS platforms_supporting
FROM <blockchain_name>.nft.ez_nft_sales
WHERE block_timestamp >= CURRENT_DATE - 7
    AND price_usd IS NOT NULL
GROUP BY event_type
ORDER BY total_volume_usd DESC;

-- Event type preferences by platform
SELECT 
    platform_name,
    event_type,
    COUNT(*) AS count,
    SUM(price_usd) AS volume_usd
FROM <blockchain_name>.nft.ez_nft_sales
WHERE block_timestamp >= CURRENT_DATE - 30
    AND price_usd IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 4 DESC;
```

### Notes
- 'sale' represents sales from direct listings
- 'bid_won' indicates seller accepting an offer on their listing 
- Some platforms support unique event types

{% enddocs %}

{% docs ez_nft_sales_platform_address %}

## Platform Address
The smart contract address of the marketplace facilitating the sale.

### Details
- **Type**: `VARCHAR(42)`
- **Format**: `0x` prefixed Ethereum address
- **Purpose**: Identifies the exact marketplace contract

### Usage Examples
```sql
-- Most active marketplace contracts
SELECT 
    platform_address,
    platform_name,
    platform_exchange_version,
    COUNT(*) AS sales_count,
    SUM(price_usd) AS total_volume_usd
FROM <blockchain_name>.nft.ez_nft_sales
WHERE block_timestamp >= CURRENT_DATE - 7
    AND price_usd IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 5 DESC;

-- Platform contract evolution
SELECT 
    platform_name,
    COUNT(DISTINCT platform_address) AS contract_versions,
    ARRAY_AGG(DISTINCT platform_address) AS contract_addresses,
    MIN(block_timestamp) AS first_seen,
    MAX(block_timestamp) AS last_seen
FROM <blockchain_name>.nft.ez_nft_sales
GROUP BY platform_name
HAVING COUNT(DISTINCT platform_address) > 1
ORDER BY contract_versions DESC;
```

### Notes
- Platforms may have multiple contracts for different versions. Platforms that facilitate buying from NFT Pools such as Sudoswap and NFTx, create a new contract address for each pool. 
- Aggregators interact with multiple platform addresses. It is possible for 1 transaction to have a sale across multiple platforms.
- Contract upgrades result in new addresses in most cases 

{% enddocs %}

{% docs ez_nft_sales_platform_name %}

## Platform Name
The marketplace or platform where the NFT sale occurred.

### Details
- **Type**: `VARCHAR`
- **Common values**: `'opensea'`, `'blur'`, `'looksrare'`, `'x2y2'`, `'rarible'`
- **Standardization**: Lowercase, no spaces

### Usage Examples
```sql
-- Platform market share over time
WITH daily_platform_volume AS (
    SELECT 
        DATE_TRUNC('day', block_timestamp) AS date,
        platform_name,
        SUM(price_usd) AS daily_volume_usd
    FROM <blockchain_name>.nft.ez_nft_sales
    WHERE block_timestamp >= CURRENT_DATE - 30
        AND price_usd IS NOT NULL
    GROUP BY 1, 2
),
daily_totals AS (
    SELECT 
        date,
        SUM(daily_volume_usd) AS total_daily_volume
    FROM daily_platform_volume
    GROUP BY date
)
SELECT 
    p.date,
    p.platform_name,
    p.daily_volume_usd,
    p.daily_volume_usd * 100.0 / t.total_daily_volume AS market_share_pct
FROM daily_platform_volume p
JOIN daily_totals t ON p.date = t.date
WHERE p.platform_name IN ('opensea', 'blur', 'looksrare', 'x2y2')
ORDER BY p.date DESC, market_share_pct DESC;

-- Cross-platform trader analysis
WITH trader_platforms AS (
    SELECT 
        buyer_address,
        ARRAY_AGG(DISTINCT platform_name) AS platforms_used,
        COUNT(DISTINCT platform_name) AS platform_count
    FROM <blockchain_name>.nft.ez_nft_sales
    WHERE block_timestamp >= CURRENT_DATE - 30
    GROUP BY buyer_address
)
SELECT 
    platform_count,
    COUNT(*) AS trader_count,
    MODE() WITHIN GROUP (ORDER BY platforms_used) AS common_platform_combo
FROM trader_platforms
GROUP BY platform_count
ORDER BY platform_count;
```

{% enddocs %}

{% docs ez_nft_sales_platform_exchange_version %}

## Platform Exchange Version
The version identifier of the marketplace contract.

### Details
- **Type**: `VARCHAR`
- **Examples**: `'seaport_1_5'`, `'blur_v2'`, `'looksrare_v1'`
- **Purpose**: Tracks protocol versions and upgrades

### Usage Examples
```sql
-- Version adoption tracking
SELECT 
    platform_name,
    platform_exchange_version,
    MIN(block_timestamp) AS version_launch_date,
    COUNT(*) AS total_sales,
    SUM(price_usd) AS total_volume_usd
FROM <blockchain_name>.nft.ez_nft_sales
WHERE price_usd IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 3;

-- Feature comparison across versions
SELECT 
    platform_exchange_version,
    AVG(total_fees / NULLIF(price, 0) * 100) AS avg_total_fee_pct,
    COUNT(DISTINCT currency_address) AS supported_currencies,
    COUNT(CASE WHEN aggregator_name IS NOT NULL THEN 1 END) * 100.0 / COUNT(*) AS aggregator_usage_pct
FROM <blockchain_name>.nft.ez_nft_sales
WHERE platform_name = 'opensea'
    AND price > 0
GROUP BY platform_exchange_version
ORDER BY platform_exchange_version;
```

{% enddocs %}

{% docs ez_nft_sales_aggregator_name %}

## Aggregator Name
The NFT aggregator platform that routed the transaction.

### Details
- **Type**: `VARCHAR`
- **Common values**: `'gem'`, `'genie'`, `'reservoir'`, `NULL` (direct sales)
- **Purpose**: Identifies intermediary platforms

### Usage Examples
```sql
-- Aggregator market share
SELECT 
    COALESCE(aggregator_name, 'Direct') AS sale_route,
    COUNT(*) AS sales_count,
    SUM(price_usd) AS total_volume_usd,
    COUNT(DISTINCT buyer_address) AS unique_buyers,
    AVG(price_usd) AS avg_sale_price
FROM <blockchain_name>.nft.ez_nft_sales
WHERE block_timestamp >= CURRENT_DATE - 30
    AND price_usd IS NOT NULL
GROUP BY 1
ORDER BY 3 DESC;

-- Aggregator routing patterns
SELECT 
    aggregator_name,
    platform_name,
    COUNT(*) AS routed_sales,
    SUM(price_usd) AS routed_volume_usd
FROM <blockchain_name>.nft.ez_nft_sales
WHERE aggregator_name IS NOT NULL
    AND block_timestamp >= CURRENT_DATE - 7
    AND price_usd IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 4 DESC;
```

### Notes
- NULL indicates direct platform interaction
- Aggregators may add additional fees 
- Some aggregators specialize in bulk purchases

{% enddocs %}

{% docs ez_nft_sales_seller_address %}

## Seller Address
The address that sold the NFT.

### Details
- **Type**: `VARCHAR(42)`
- **Format**: `0x` prefixed Ethereum address
- **Purpose**: Identifies the NFT seller

### Usage Examples
```sql
-- Top NFT sellers by volume
SELECT 
    seller_address,
    COUNT(*) AS sales_count,
    COUNT(DISTINCT contract_address) AS collections_sold,
    SUM(price_usd) AS total_revenue_usd,
    AVG(price_usd) AS avg_sale_price,
    SUM(price_usd - COALESCE(total_fees_usd, 0)) AS net_revenue_usd
FROM <blockchain_name>.nft.ez_nft_sales
WHERE block_timestamp >= CURRENT_DATE - 30
    AND price_usd IS NOT NULL
GROUP BY seller_address
ORDER BY total_revenue_usd DESC
LIMIT 100;

-- Seller behavior patterns
WITH seller_metrics AS (
    SELECT 
        seller_address,
        COUNT(*) AS total_sales,
        COUNT(DISTINCT DATE_TRUNC('day', block_timestamp)) AS active_days,
        COUNT(DISTINCT platform_name) AS platforms_used
    FROM <blockchain_name>.nft.ez_nft_sales
    WHERE block_timestamp >= CURRENT_DATE - 90
    GROUP BY seller_address
)
SELECT 
    CASE 
        WHEN total_sales = 1 THEN 'One-time Seller'
        WHEN total_sales <= 10 THEN 'Occasional Seller'
        WHEN total_sales <= 100 THEN 'Active Seller'
        ELSE 'Power Seller'
    END AS seller_type,
    COUNT(*) AS seller_count,
    AVG(total_sales) AS avg_sales,
    AVG(active_days) AS avg_active_days,
    AVG(platforms_used) AS avg_platforms
FROM seller_metrics
GROUP BY seller_type
ORDER BY MIN(total_sales);
```

{% enddocs %}

{% docs ez_nft_sales_buyer_address %}

## Buyer Address
The address that purchased the NFT.

### Details
- **Type**: `VARCHAR(42)`
- **Format**: `0x` prefixed Ethereum address
- **Purpose**: Identifies the NFT buyer

### Usage Examples
```sql
-- Buyer accumulation patterns
WITH buyer_purchases AS (
    SELECT 
        buyer_address,
        contract_address,
        name AS collection_name,
        COUNT(*) AS tokens_bought,
        SUM(price_usd) AS total_spent_usd,
        AVG(price_usd) AS avg_price_paid
    FROM <blockchain_name>.nft.ez_nft_sales
    WHERE block_timestamp >= CURRENT_DATE - 30
        AND price_usd IS NOT NULL
    GROUP BY 1, 2, 3
)
SELECT 
    buyer_address,
    COUNT(DISTINCT contract_address) AS collections_count,
    SUM(tokens_bought) AS total_nfts_bought,
    SUM(total_spent_usd) AS total_invested_usd,
    ARRAY_AGG(collection_name ORDER BY total_spent_usd DESC LIMIT 3) AS top_collections
FROM buyer_purchases
GROUP BY buyer_address
HAVING SUM(total_spent_usd) > 10000
ORDER BY total_invested_usd DESC
LIMIT 100;

-- New vs returning buyers
WITH buyer_history AS (
    SELECT 
        buyer_address,
        MIN(block_timestamp) AS first_purchase,
        MAX(block_timestamp) AS last_purchase,
        COUNT(*) AS total_purchases
    FROM <blockchain_name>.nft.ez_nft_sales
    GROUP BY buyer_address
)
SELECT 
    DATE_TRUNC('day', s.block_timestamp) AS date,
    COUNT(DISTINCT CASE 
        WHEN DATE_TRUNC('day', h.first_purchase) = DATE_TRUNC('day', s.block_timestamp) 
        THEN s.buyer_address 
    END) AS new_buyers,
    COUNT(DISTINCT CASE 
        WHEN DATE_TRUNC('day', h.first_purchase) < DATE_TRUNC('day', s.block_timestamp) 
        THEN s.buyer_address 
    END) AS returning_buyers,
    SUM(s.price_usd) AS total_volume_usd
FROM <blockchain_name>.nft.ez_nft_sales s
JOIN buyer_history h ON s.buyer_address = h.buyer_address
WHERE s.block_timestamp >= CURRENT_DATE - 30
    AND s.price_usd IS NOT NULL
GROUP BY date
ORDER BY date DESC;
```

{% enddocs %}

{% docs ez_nft_sales_contract_address %}

## Contract Address
The smart contract address of the NFT collection.

### Details
- **Type**: `VARCHAR(42)`
- **Format**: `0x` prefixed Ethereum address
- **Standards**: ERC-721, ERC-1155, or custom implementations

### Usage Examples
```sql
-- Collection performance metrics
SELECT 
    contract_address,
    name,
    COUNT(*) AS total_sales,
    COUNT(DISTINCT token_id) AS unique_tokens_traded,
    COUNT(DISTINCT buyer_address) AS unique_buyers,
    COUNT(DISTINCT seller_address) AS unique_sellers,
    SUM(price_usd) AS total_volume_usd,
    AVG(price_usd) AS avg_price_usd,
    STDDEV(price_usd) AS price_volatility
FROM <blockchain_name>.nft.ez_nft_sales
WHERE block_timestamp >= CURRENT_DATE - 30
    AND price_usd IS NOT NULL
GROUP BY 1, 2
HAVING COUNT(*) > 50
ORDER BY total_volume_usd DESC;

-- Collection holder concentration
WITH current_holders AS (
    SELECT 
        contract_address,
        buyer_address AS holder,
        COUNT(DISTINCT token_id) AS tokens_held
    FROM <blockchain_name>.nft.ez_nft_sales s1
    WHERE NOT EXISTS (
        SELECT 1 
        FROM <blockchain_name>.nft.ez_nft_sales s2 
        WHERE s2.contract_address = s1.contract_address
            AND s2.token_id = s1.token_id
            AND s2.seller_address = s1.buyer_address
            AND s2.block_timestamp > s1.block_timestamp
    )
    GROUP BY 1, 2
)
SELECT 
    contract_address,
    COUNT(DISTINCT holder) AS total_holders,
    SUM(tokens_held) AS total_tokens,
    MAX(tokens_held) AS largest_holder_tokens
FROM current_holders
GROUP BY contract_address
HAVING COUNT(DISTINCT holder) > 100
ORDER BY largest_holder_tokens DESC;
```
- Note that NFTs can still be transferrred to another wallet without requiring an NFT sale 

{% enddocs %}

{% docs ez_nft_sales_name %}

## Name
The name of the NFT collection or project.

### Details
- **Type**: `VARCHAR`
- **Source**: Contract metadata or manual curation
- **Examples**: `'Bored Ape Yacht Club'`, `'CryptoPunks'`, `'Art Blocks'`

### Usage Examples
```sql
-- Trending collections (momentum analysis)
WITH collection_daily_volume AS (
    SELECT 
        DATE_TRUNC('day', block_timestamp) AS date,
        name,
        contract_address,
        SUM(price_usd) AS daily_volume_usd,
        COUNT(*) AS daily_sales
    FROM <blockchain_name>.nft.ez_nft_sales
    WHERE block_timestamp >= CURRENT_DATE - 14
        AND price_usd IS NOT NULL
        AND name IS NOT NULL
    GROUP BY 1, 2, 3
),
collection_momentum AS (
    SELECT 
        name,
        contract_address,
        SUM(CASE WHEN date >= CURRENT_DATE - 7 THEN daily_volume_usd ELSE 0 END) AS volume_last_7d,
        SUM(CASE WHEN date < CURRENT_DATE - 7 THEN daily_volume_usd ELSE 0 END) AS volume_prev_7d,
        SUM(CASE WHEN date >= CURRENT_DATE - 7 THEN daily_sales ELSE 0 END) AS sales_last_7d
    FROM collection_daily_volume
    GROUP BY 1, 2
    HAVING SUM(CASE WHEN date < CURRENT_DATE - 7 THEN daily_volume_usd ELSE 0 END) > 0
)
SELECT 
    name,
    contract_address,
    volume_last_7d,
    volume_prev_7d,
    (volume_last_7d / volume_prev_7d - 1) * 100 AS volume_change_pct,
    sales_last_7d
FROM collection_momentum
WHERE volume_last_7d > 10000
ORDER BY volume_change_pct DESC
LIMIT 50;
```

### Notes
- Names may be NULL for unverified collections
- Some collections have similar names (check contract_address)
- Names are standardized when possible

{% enddocs %}

{% docs ez_nft_sales_token_id %}

## Token ID
The unique identifier of the specific NFT within its collection.

### Details
- **Type**: `VARCHAR`
- **Format**: Numeric string (can be very large)
- **Uniqueness**: Unique within a contract_address

### Usage Examples
```sql
-- Rare token premium analysis
WITH token_sales AS (
    SELECT 
        contract_address,
        name,
        token_id,
        COUNT(*) AS sale_count,
        AVG(price_usd) AS avg_price_usd,
        MAX(price_usd) AS max_price_usd
    FROM <blockchain_name>.nft.ez_nft_sales
    WHERE price_usd IS NOT NULL
        AND block_timestamp >= CURRENT_DATE - 90
    GROUP BY 1, 2, 3
),
collection_avg AS (
    SELECT 
        contract_address,
        AVG(avg_price_usd) AS collection_avg_price
    FROM token_sales
    GROUP BY contract_address
)
SELECT 
    t.name,
    t.token_id,
    t.sale_count,
    t.avg_price_usd,
    c.collection_avg_price,
    (t.avg_price_usd / c.collection_avg_price - 1) * 100 AS premium_pct
FROM token_sales t
JOIN collection_avg c ON t.contract_address = c.contract_address
WHERE t.sale_count > 1
    AND t.avg_price_usd > c.collection_avg_price * 2
ORDER BY premium_pct DESC
LIMIT 100;

-- Token velocity (flipping activity)
WITH token_flips AS (
    SELECT 
        contract_address,
        token_id,
        seller_address,
        buyer_address,
        price_usd,
        block_timestamp,
        LAG(price_usd) OVER (PARTITION BY contract_address, token_id ORDER BY block_timestamp) AS prev_price,
        LAG(block_timestamp) OVER (PARTITION BY contract_address, token_id ORDER BY block_timestamp) AS prev_sale_time
    FROM <blockchain_name>.nft.ez_nft_sales
    WHERE price_usd IS NOT NULL
)
SELECT 
    contract_address,
    token_id,
    COUNT(*) AS flip_count,
    AVG(TIMESTAMPDIFF('HOUR', prev_sale_time, block_timestamp )) AS avg_hold_time_hours,
    SUM(CASE WHEN price_usd > prev_price THEN 1 ELSE 0 END) AS profitable_flips,
    SUM(price_usd - prev_price) AS total_pnl_usd
FROM token_flips
WHERE prev_price IS NOT NULL
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY 1, 2
HAVING COUNT(*) > 2
ORDER BY flip_count DESC;
```

{% enddocs %}

{% docs ez_nft_sales_quantity %}

## Quantity
The number of tokens sold in the transaction (for ERC-1155).

### Details
- **Type**: `VARCHAR`
- **Usage**: Do not use `> 1` to identify ERC-1155 sales. Instead, use `token_standard` and filter it to "erc1155"

### Usage Examples
```sql
-- ERC-1155 batch sale analysis
SELECT 
    name,
    contract_address,
    token_standard,
    SUM(quantity) AS total_tokens_sold,
    COUNT(*) AS sale_transactions,
    AVG(quantity) AS avg_batch_size,
    MAX(quantity) AS largest_batch
FROM <blockchain_name>.nft.ez_nft_sales
WHERE token_standard = 'erc1155'
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY 1, 2, 3
ORDER BY total_tokens_sold DESC;

-- Price per unit analysis for batch sales
SELECT 
    name,
    token_id,
    quantity,
    price,
    price_usd,
    price / quantity AS price_per_unit,
    price_usd / quantity AS price_per_unit_usd
FROM <blockchain_name>.nft.ez_nft_sales
WHERE token_standard = 'erc1155'
    AND price > 0
    AND block_timestamp >= CURRENT_DATE - 7
ORDER BY price_usd DESC
LIMIT 100;
```

{% enddocs %}

{% docs ez_nft_sales_token_standard %}

## Token Standard
The technical standard implemented by the NFT contract.

### Details
- **Type**: `VARCHAR`
- **Common values**: `'erc721'`, `'erc1155'`, `'cryptopunks'`, `'legacy'`
- **Purpose**: Identifies NFT contract type.
- **Usage** 'Cryptopunks' and 'legacy' represent old NFT token standards

### Usage Examples
```sql
-- Market share by token standard
SELECT 
    token_standard,
    COUNT(*) AS sales_count,
    COUNT(DISTINCT contract_address) AS unique_collections,
    SUM(price_usd) AS total_volume_usd,
    AVG(price_usd) AS avg_price_usd
FROM <blockchain_name>.nft.ez_nft_sales
WHERE block_timestamp >= CURRENT_DATE - 30
    AND price_usd IS NOT NULL
GROUP BY token_standard
ORDER BY total_volume_usd DESC;

-- Platform support for standards
SELECT 
    platform_name,
    token_standard,
    COUNT(*) AS sales_count,
    COUNT(DISTINCT contract_address) AS collections_supported
FROM <blockchain_name>.nft.ez_nft_sales
WHERE block_timestamp >= CURRENT_DATE - 30
GROUP BY 1, 2
ORDER BY 1, 3 DESC;
```

### Notes
- ERC-721: One token per token_id
- ERC-1155: Multiple editions per token_id
- Some collections use custom standards

{% enddocs %}

{% docs ez_nft_sales_currency_symbol %}

## Currency Symbol
The symbol of the token used for payment.

### Details
- **Type**: `VARCHAR`
- **Common values**: `'ETH'`, `'WETH'`, `'USDC'`, `'DAI'`, platform tokens
- **Purpose**: Identifies payment currency

### Usage Examples
```sql
-- Payment currency preferences
SELECT 
    currency_symbol,
    COUNT(*) AS sales_count,
    SUM(price_usd) AS total_volume_usd,
    AVG(price_usd) AS avg_sale_usd,
    COUNT(DISTINCT buyer_address) AS unique_buyers
FROM <blockchain_name>.nft.ez_nft_sales
WHERE block_timestamp >= CURRENT_DATE - 30
    AND price_usd IS NOT NULL
    AND currency_symbol IS NOT NULL
GROUP BY currency_symbol
ORDER BY total_volume_usd DESC;

-- Currency usage by price tier
SELECT 
    CASE 
        WHEN price_usd < 100 THEN '< $100'
        WHEN price_usd < 1000 THEN '$100 - $1K'
        WHEN price_usd < 10000 THEN '$1K - $10K'
        ELSE '> $10K'
    END AS price_tier,
    currency_symbol,
    COUNT(*) AS sales_count,
    SUM(price_usd) AS volume_usd
FROM <blockchain_name>.nft.ez_nft_sales
WHERE price_usd IS NOT NULL
    AND currency_symbol IS NOT NULL
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY 1, 2
ORDER BY 1, 4 DESC;
```

{% enddocs %}

{% docs ez_nft_sales_currency_address %}

## Currency Address
The contract address of the payment token.

### Details
- **Type**: `VARCHAR(42)`
- **Format**: `0x` prefixed address, 'ETH' for native ETH
- **Purpose**: Identifies exact payment token. 
- **Usage**: Use `currency_address` over `currency_symbol` when filtering for a particular payment token contract.

### Usage Examples
```sql
-- ERC-20 payment token analysis
SELECT 
    currency_address,
    currency_symbol,
    COUNT(*) AS usage_count,
    SUM(price_usd) AS total_volume_usd,
    COUNT(DISTINCT platform_name) AS platforms_accepting
FROM <blockchain_name>.nft.ez_nft_sales
WHERE currency_address IS NOT NULL
    AND block_timestamp >= CURRENT_DATE - 30
    AND price_usd IS NOT NULL
GROUP BY 1, 2
ORDER BY total_volume_usd DESC;

-- Native ETH vs ERC-20 payments
SELECT 
    CASE 
        WHEN currency_address = 'ETH' THEN 'Native ETH'
        ELSE 'ERC-20 Token'
    END AS payment_type,
    COUNT(*) AS sales_count,
    SUM(price_usd) AS total_volume_usd,
    AVG(total_fees / NULLIF(price, 0) * 100) AS avg_fee_pct
FROM <blockchain_name>.nft.ez_nft_sales
WHERE block_timestamp >= CURRENT_DATE - 30
    AND price > 0
GROUP BY payment_type;
```

{% enddocs %}

{% docs ez_nft_sales_price %}

## Price
The sale price in the payment currency which includes the platform and creator fees if any.

### Details
- **Type**: `NUMERIC`
- **Precision**: Decimal adjusted for token decimals
- **Usage**: Raw price before USD conversion


### Usage Examples
```sql
-- Price distribution by currency
SELECT 
    currency_symbol,
    MIN(price) AS min_price,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price) AS p25,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price) AS p75,
    MAX(price) AS max_price,
    AVG(price) AS avg_price
FROM <blockchain_name>.nft.ez_nft_sales
WHERE price > 0
    AND currency_symbol IN ('ETH', 'WETH')
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY currency_symbol;

-- High-value sales
SELECT 
    block_timestamp,
    tx_hash,
    name,
    token_id,
    price,
    currency_symbol,
    price_usd,
    platform_name
FROM <blockchain_name>.nft.ez_nft_sales
WHERE price > 100
    AND currency_symbol IN ('ETH', 'WETH')
    AND block_timestamp >= CURRENT_DATE - 7
ORDER BY price DESC
LIMIT 50;
```

{% enddocs %}

{% docs ez_nft_sales_price_usd %}

## Price USD
The sale price converted to USD at transaction time which includes the platform and creator fees if any.

### Details
- **Type**: `NUMERIC`
- **Source**: Historical price feeds
- **NULL cases**: Missing price data for currency

### Usage Examples
```sql
-- Daily market statistics
SELECT 
    DATE_TRUNC('day', block_timestamp) AS date,
    COUNT(*) AS total_sales,
    SUM(price_usd) AS daily_volume_usd,
    AVG(price_usd) AS avg_sale_price_usd,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_usd) AS median_price_usd,
    MAX(price_usd) AS highest_sale_usd
FROM <blockchain_name>.nft.ez_nft_sales
WHERE price_usd IS NOT NULL
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY date
ORDER BY date DESC;

-- Price tier analysis
SELECT 
    CASE 
        WHEN price_usd < 100 THEN '1. Micro (< $100)'
        WHEN price_usd < 1000 THEN '2. Small ($100-$1K)'
        WHEN price_usd < 10000 THEN '3. Medium ($1K-$10K)'
        WHEN price_usd < 100000 THEN '4. Large ($10K-$100K)'
        WHEN price_usd < 1000000 THEN '5. Whale ($100K-$1M)'
        ELSE '6. Mega (> $1M)'
    END AS price_tier,
    COUNT(*) AS sales_count,
    SUM(price_usd) AS tier_volume_usd,
    COUNT(DISTINCT buyer_address) AS unique_buyers,
    COUNT(DISTINCT contract_address) AS unique_collections
FROM <blockchain_name>.nft.ez_nft_sales
WHERE price_usd IS NOT NULL
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY price_tier
ORDER BY price_tier;
```

### Performance Notes
- Always check for NULL when calculating totals
- USD conversion happens at block timestamp
- Large sales should be verified for accuracy

{% enddocs %}

{% docs ez_nft_sales_total_fees %}

## Total Fees
The combined platform and creator fees in the payment currency.

### Details
- **Type**: `NUMERIC`
- **Calculation**: `platform_fee + creator_fee`
- **Purpose**: Total cost beyond sale price

### Usage Examples
```sql
-- Fee analysis by platform
SELECT 
    platform_name,
    AVG(total_fees / NULLIF(price, 0) * 100) AS avg_total_fee_pct,
    AVG(platform_fee / NULLIF(price, 0) * 100) AS avg_platform_fee_pct,
    AVG(creator_fee / NULLIF(price, 0) * 100) AS avg_creator_fee_pct,
    SUM(total_fees_usd) AS total_fees_collected_usd
FROM <blockchain_name>.nft.ez_nft_sales
WHERE price > 0
    AND total_fees >= 0
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY platform_name
ORDER BY total_fees_collected_usd DESC;

-- Net proceeds to sellers
SELECT 
    seller_address,
    COUNT(*) AS sales,
    SUM(price) AS gross_revenue,
    SUM(total_fees) AS fees_paid,
    SUM(price - COALESCE(total_fees, 0)) AS net_revenue,
    AVG(total_fees / NULLIF(price, 0) * 100) AS avg_fee_rate_pct
FROM <blockchain_name>.nft.ez_nft_sales
WHERE price > 0
    AND currency_symbol = 'ETH'
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY seller_address
HAVING COUNT(*) > 5
ORDER BY net_revenue DESC
LIMIT 100;
```

{% enddocs %}

{% docs ez_nft_sales_platform_fee %}

## Platform Fee
The fee charged by the marketplace in the payment currency.

### Details
- **Type**: `NUMERIC`
- **Range**: Typically 2-2.5% of sale price
- **Purpose**: Marketplace revenue

### Usage Examples
```sql
-- Platform fee revenue analysis
SELECT 
    platform_name,
    DATE_TRUNC('week', block_timestamp) AS week,
    SUM(platform_fee_usd) AS weekly_revenue_usd,
    COUNT(*) AS transactions,
    AVG(platform_fee / NULLIF(price, 0) * 100) AS avg_fee_rate_pct
FROM <blockchain_name>.nft.ez_nft_sales
WHERE platform_fee > 0
    AND price > 0
    AND block_timestamp >= CURRENT_DATE - 90
GROUP BY 1, 2
ORDER BY 1, 2 DESC;

-- Platform fee structures comparison
SELECT 
    platform_name,
    platform_exchange_version,
    ROUND(platform_fee / NULLIF(price, 0) * 100, 2) AS fee_pct,
    COUNT(*) AS sales_count,
    SUM(platform_fee_usd) AS total_revenue_usd
FROM <blockchain_name>.nft.ez_nft_sales
WHERE platform_fee > 0
    AND price > 0
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
```

{% enddocs %}

{% docs ez_nft_sales_creator_fee %}

## Creator Fee
The royalty fee paid to the collection creator in the payment currency.

### Details
- **Type**: `NUMERIC`
- **Range**: Typically 0-10% of sale price
- **Enforcement**: Varies by marketplace

### Usage Examples
```sql
-- Creator royalty analysis by collection
SELECT 
    name,
    contract_address,
    COUNT(*) AS sales_with_royalties,
    SUM(creator_fee_usd) AS total_royalties_usd,
    AVG(creator_fee / NULLIF(price, 0) * 100) AS avg_royalty_rate_pct,
    COUNT(CASE WHEN creator_fee = 0 THEN 1 END) * 100.0 / COUNT(*) AS zero_royalty_pct
FROM <blockchain_name>.nft.ez_nft_sales
WHERE price > 0
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY 1, 2
HAVING COUNT(*) > 20
ORDER BY total_royalties_usd DESC;

-- Royalty enforcement by platform
SELECT 
    platform_name,
    COUNT(CASE WHEN creator_fee > 0 THEN 1 END) * 100.0 / COUNT(*) AS royalty_enforcement_pct,
    AVG(CASE WHEN creator_fee > 0 THEN creator_fee / NULLIF(price, 0) * 100 END) AS avg_royalty_when_paid_pct,
    SUM(creator_fee_usd) AS total_royalties_facilitated_usd
FROM <blockchain_name>.nft.ez_nft_sales
WHERE price > 0
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY platform_name
ORDER BY royalty_enforcement_pct DESC;
```

### Notes
- Some platforms allow optional royalties
- Creator fees may be 0 on certain platforms
- Enforcement has changed over time

{% enddocs %}

{% docs ez_nft_sales_total_fees_usd %}

## Total Fees USD
The combined platform and creator fees converted to USD.

### Details
- **Type**: `NUMERIC`
- **Calculation**: `platform_fee_usd + creator_fee_usd`
- **Purpose**: Total fees in dollar terms

### Usage Examples
```sql
-- Fee burden analysis by sale size
SELECT 
    CASE 
        WHEN price_usd < 1000 THEN '< $1K'
        WHEN price_usd < 10000 THEN '$1K-$10K'
        WHEN price_usd < 100000 THEN '$10K-$100K'
        ELSE '> $100K'
    END AS sale_tier,
    COUNT(*) AS sales,
    AVG(total_fees_usd / NULLIF(price_usd, 0) * 100) AS avg_fee_pct,
    SUM(total_fees_usd) AS total_fees_collected_usd,
    SUM(price_usd - COALESCE(total_fees_usd, 0)) AS net_to_sellers_usd
FROM <blockchain_name>.nft.ez_nft_sales
WHERE price_usd > 0
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY sale_tier
ORDER BY MIN(price_usd);
```

{% enddocs %}

{% docs ez_nft_sales_platform_fee_usd %}

## Platform Fee USD
The marketplace fee converted to USD.

### Details
- **Type**: `NUMERIC`
- **Purpose**: Platform revenue in dollar terms
- **Usage**: Financial analysis and reporting

### Usage Examples
```sql
-- Monthly platform revenue trends
SELECT 
    platform_name,
    DATE_TRUNC('month', block_timestamp) AS month,
    SUM(platform_fee_usd) AS monthly_revenue_usd,
    COUNT(*) AS transactions,
    COUNT(DISTINCT buyer_address) AS unique_buyers
FROM <blockchain_name>.nft.ez_nft_sales
WHERE platform_fee_usd > 0
    AND block_timestamp >= CURRENT_DATE - 365
GROUP BY 1, 2
ORDER BY 1, 2 DESC;

-- Platform revenue per user
SELECT 
    platform_name,
    COUNT(DISTINCT buyer_address) AS unique_buyers,
    SUM(platform_fee_usd) AS total_revenue_usd,
    SUM(platform_fee_usd) / COUNT(DISTINCT buyer_address) AS revenue_per_buyer_usd
FROM <blockchain_name>.nft.ez_nft_sales
WHERE platform_fee_usd > 0
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY platform_name
ORDER BY revenue_per_buyer_usd DESC;
```

{% enddocs %}

{% docs ez_nft_sales_creator_fee_usd %}

## Creator Fee USD
The royalty fee converted to USD.

### Details
- **Type**: `NUMERIC`
- **Purpose**: Creator earnings in dollar terms
- **Variability**: Depends on marketplace policies

### Usage Examples
```sql
-- Top earning collections by royalties
SELECT 
    name,
    contract_address,
    SUM(creator_fee_usd) AS total_royalties_earned_usd,
    COUNT(*) AS royalty_bearing_sales,
    AVG(creator_fee_usd) AS avg_royalty_per_sale_usd,
    MAX(creator_fee_usd) AS highest_single_royalty_usd
FROM <blockchain_name>.nft.ez_nft_sales
WHERE creator_fee_usd > 0
    AND block_timestamp >= CURRENT_DATE - 90
GROUP BY 1, 2
ORDER BY total_royalties_earned_usd DESC
LIMIT 50;

-- Royalty trends over time
SELECT 
    DATE_TRUNC('week', block_timestamp) AS week,
    SUM(creator_fee_usd) AS weekly_royalties_usd,
    SUM(creator_fee_usd) / NULLIF(SUM(price_usd), 0) * 100 AS royalty_rate_pct,
    COUNT(CASE WHEN creator_fee_usd > 0 THEN 1 END) AS royalty_sales,
    COUNT(*) AS total_sales
FROM <blockchain_name>.nft.ez_nft_sales
WHERE price_usd > 0
    AND block_timestamp >= CURRENT_DATE - 180
GROUP BY week
ORDER BY week DESC;
```

{% enddocs %}

{% docs ez_nft_sales_tx_fee_usd %}

## Transaction Fee USD
The transaction fee denominated in USD 

### Details
- **Type**: `NUMERIC`
- **Purpose**: Transaction fee in dollar terms
- **Usage**: If there are multiple sales in one transaction, use only one instance per transaction to avoid overcounting 

### Usage Examples
```sql
-- Highest transaction fees paid 
SELECT 
    contract_address,
    name,
    tx_fee_usd,
    price_usd 
FROM ethereum.nft.ez_nft_sales
WHERE block_timestamp >= CURRENT_DATE - 90
QUALIFY ROW_NUMBER() OVER (PARTITION BY tx_hash ORDER BY event_index DESC) = 1 
ORDER BY 3 DESC 
LIMIT 50
```

{% enddocs %}