{% docs ez_nft_sales_table_doc %}

## What

This table provides a comprehensive view of NFT (Non-Fungible Token) sales across all major marketplaces and platforms on EVM blockchains. It captures both direct marketplace sales and aggregator-routed transactions, enabling analysis of NFT market dynamics, collection performance, and trading patterns.

## Key Use Cases

- Analyze daily/weekly NFT market volume and trends by platform
- Track top-performing NFT collections by sales count and volume
- Monitor marketplace competition and market share analysis
- Identify whale activity and buyer behavior patterns
- Evaluate creator royalty enforcement across platforms
- Assess fee structures and revenue models by marketplace
- Track cross-platform trader behavior and platform preferences

## Important Relationships

- Links to `core.fact_event_logs` via `tx_hash` and `event_index`
- Joins with `core.dim_contracts` for collection name
- References `price.ez_prices_hourly` for currency conversions
- Connects to token transfer tables for ownership tracking

## Commonly-used Fields

- `platform_name`: Marketplace where sale occurred (opensea, blur, etc.)
- `contract_address`: NFT collection contract address
- `token_id`: Unique identifier of the specific NFT
- `buyer_address` / `seller_address`: Transaction participants
- `price_usd`: Sale price converted to USD (includes fees)
- `total_fees_usd`: Combined platform and creator fees in USD
- `event_type`: Type of sale transaction (sale, bid_won, etc.)

## Sample Queries

**Daily NFT market volume by platform**
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
```

**Top selling NFT collections**
```sql
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
```

**Marketplace competition analysis**
```sql
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
```

**Whale activity tracking**
```sql
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
```
    
**Creator royalty analysis**
```sql
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

{% enddocs %}

{% docs ez_nft_sales_event_type %}

The specific type of NFT transaction that occurred. Common values include 'sale', 'bid_won', 'redeem', and 'mint'.

Example: 'sale'

{% enddocs %}

{% docs ez_nft_sales_platform_address %}

The smart contract address of the marketplace facilitating the sale. Platforms may have multiple contracts for different versions.

Example: '0x00000000006c3852cbef3e08e8df289169ede581'

{% enddocs %}

{% docs ez_nft_sales_platform_name %}

The marketplace or platform where the NFT sale occurred. Standardized to lowercase with no spaces.

Example: 'opensea'

{% enddocs %}

{% docs ez_nft_sales_platform_exchange_version %}

The version identifier of the marketplace contract. Tracks protocol versions and upgrades.

Example: 'seaport_1_5'

{% enddocs %}

{% docs ez_nft_sales_aggregator_name %}

The NFT aggregator platform that routed the transaction. NULL indicates direct platform interaction.

Example: 'gem'

{% enddocs %}

{% docs ez_nft_sales_seller_address %}

The blockchain address that sold the NFT.

Example: '0x1234567890123456789012345678901234567890'

{% enddocs %}

{% docs ez_nft_sales_buyer_address %}

The blockchain address that purchased the NFT.

Example: '0x1234567890123456789012345678901234567890'

{% enddocs %}

{% docs ez_nft_sales_contract_address %}

The smart contract address of the NFT collection. Supports ERC-721, ERC-1155, and custom implementations.

Example: '0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d'

{% enddocs %}

{% docs ez_nft_sales_name %}

The name of the NFT collection or project. May be NULL for unverified collections.

Example: 'Bored Ape Yacht Club'

{% enddocs %}

{% docs ez_nft_sales_token_id %}

The unique identifier of the specific NFT within its collection. Format is numeric string.

Example: '1234'

{% enddocs %}

{% docs ez_nft_sales_quantity %}

The number of tokens sold in the transaction. Primarily relevant for ERC-1155 tokens.

Example: '1'

{% enddocs %}

{% docs ez_nft_sales_token_standard %}

The technical standard implemented by the NFT contract. Common values include 'erc721', 'erc1155', 'cryptopunks', and 'legacy'.

Example: 'erc721'

{% enddocs %}

{% docs ez_nft_sales_currency_symbol %}

The symbol of the token used for payment.

Example: 'ETH'

{% enddocs %}

{% docs ez_nft_sales_currency_address %}

The contract address of the payment token. Shows 'ETH' for native ETH payments.

Example: '0xa0b86a33e6776a1e7f9f0b8b8b8b8b8b8b8b8b8b'

{% enddocs %}

{% docs ez_nft_sales_price %}

The sale price in the payment currency, including platform and creator fees. Raw price before USD conversion.

Example: 2.5

{% enddocs %}

{% docs ez_nft_sales_price_usd %}

The sale price converted to USD at transaction time, including platform and creator fees. May be NULL for missing price data.

Example: 4250.75

{% enddocs %}

{% docs ez_nft_sales_total_fees %}

The combined platform and creator fees in the payment currency.

Example: 0.125

{% enddocs %}

{% docs ez_nft_sales_platform_fee %}

The fee charged by the marketplace in the payment currency. Typically 2-2.5% of sale price.

Example: 0.05

{% enddocs %}

{% docs ez_nft_sales_creator_fee %}

The royalty fee paid to the collection creator in the payment currency. Typically 0-10% of sale price.

Example: 0.075

{% enddocs %}

{% docs ez_nft_sales_total_fees_usd %}

The combined platform and creator fees converted to USD.

Example: 212.54

{% enddocs %}

{% docs ez_nft_sales_platform_fee_usd %}

The marketplace fee converted to USD.

Example: 85.02

{% enddocs %}

{% docs ez_nft_sales_creator_fee_usd %}

The royalty fee converted to USD.

Example: 127.52

{% enddocs %}

{% docs ez_nft_sales_tx_fee_usd %}

The transaction fee denominated in USD. Use only one instance per transaction to avoid overcounting.

Example: 45.32

{% enddocs %}