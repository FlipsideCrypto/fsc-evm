{% docs ez_prices_hourly_table_doc %}

## What

This curated table provides reliable hourly price data for tokens and native assets across EVM blockchains. It combines multiple data sources with quality checks to ensure accurate, consistent pricing for DeFi analytics, portfolio valuations, and historical analysis.

## Key Use Cases

- Calculate USD values for token transfers and transaction amounts
- Track price volatility and market movements over time
- Monitor stablecoin depegging events and price stability
- Analyze native asset price trends across different blockchains
- Perform portfolio valuations and historical price lookups
- Create price charts and technical analysis dashboards
- Assess price data quality and imputation rates

## Important Relationships

- **Join with ez_token_transfers**: Calculate transfer USD values using hourly price snapshots
- **Join with ez_asset_metadata**: Get token details and verification status
- **Join with dim_contracts**: Match addresses with contract metadata

## Commonly-used Fields

- `hour`: UTC timestamp truncated to hour for price recording
- `token_address`: Contract address of the token (NULL for native assets)
- `symbol`: Token ticker symbol (ETH, USDC, etc.)
- `price`: USD price of one whole token unit
- `is_imputed`: Flag indicating forward-filled prices due to missing data
- `is_native`: Boolean for blockchain native currencies
- `blockchain`: Network where the asset exists

## Sample Queries

**Token Price Lookup with USD Calculations**
```sql
-- Calculate USD value for token transfers
SELECT 
    t.block_timestamp,
    t.tx_hash,
    t.symbol,
    t.from_address,
    t.to_address,
    t.amount,
    p.price,
    t.amount * p.price AS usd_value,
    p.is_imputed
FROM <blockchain_name>.core.ez_token_transfers t
JOIN <blockchain_name>.price.ez_prices_hourly p
    ON t.contract_address = p.token_address
    AND DATE_TRUNC('hour', t.block_timestamp) = p.hour
WHERE t.block_timestamp >= CURRENT_DATE - 7
    AND t.amount > 0
    AND p.price IS NOT NULL
ORDER BY usd_value DESC
LIMIT 100;
```

**Price Volatility Analysis**
```sql
-- 24-hour price volatility for major tokens
WITH price_stats AS (
    SELECT 
        symbol,
        token_address,
        DATE(hour) AS date,
        MIN(price) AS daily_low,
        MAX(price) AS daily_high,
        AVG(price) AS daily_avg,
        STDDEV(price) AS daily_stddev,
        (MAX(price) - MIN(price)) / MIN(price) * 100 AS daily_range_pct
    FROM <blockchain_name>.price.ez_prices_hourly
    WHERE hour >= CURRENT_DATE - 30
        AND symbol IN ('WETH', 'USDC', 'USDT', 'WBTC', 'DAI')
        AND NOT is_imputed
    GROUP BY 1, 2, 3
)
SELECT 
    symbol,
    AVG(daily_range_pct) AS avg_daily_volatility,
    MAX(daily_range_pct) AS max_daily_volatility,
    AVG(daily_stddev / daily_avg) * 100 AS avg_cv_pct
FROM price_stats
GROUP BY 1
ORDER BY 2 DESC;
```

**Native Asset Price Tracking**
```sql
-- Track native asset prices across chains
SELECT 
    blockchain,
    symbol,
    hour,
    price,
    price / LAG(price, 24) OVER (PARTITION BY blockchain ORDER BY hour) - 1 AS change_24h,
    price / LAG(price, 168) OVER (PARTITION BY blockchain ORDER BY hour) - 1 AS change_7d
FROM <blockchain_name>.price.ez_prices_hourly
WHERE is_native = TRUE
    AND hour >= CURRENT_DATE - 8
    AND hour = DATE_TRUNC('hour', CURRENT_TIMESTAMP) - INTERVAL '1 hour'
ORDER BY blockchain;
```

**Stablecoin Peg Monitoring**
```sql
-- Monitor stablecoin deviations from $1
SELECT 
    symbol,
    hour,
    price,
    ABS(price - 1.0) AS deviation,
    CASE 
        WHEN ABS(price - 1.0) > 0.05 THEN 'Severe Depeg'
        WHEN ABS(price - 1.0) > 0.01 THEN 'Mild Depeg'
        ELSE 'Stable'
    END AS peg_status
FROM <blockchain_name>.price.ez_prices_hourly
WHERE symbol IN ('USDC', 'USDT', 'DAI', 'BUSD', 'FRAX', 'LUSD')
    AND hour >= CURRENT_DATE - 7
    AND ABS(price - 1.0) > 0.005
ORDER BY hour DESC, deviation DESC;
```

**Price Data Quality Check**
```sql
-- Analyze price data completeness and imputation rates
SELECT 
    symbol,
    COUNT(*) AS total_hours,
    SUM(CASE WHEN is_imputed THEN 1 ELSE 0 END) AS imputed_hours,
    ROUND(100.0 * SUM(CASE WHEN is_imputed THEN 1 ELSE 0 END) / COUNT(*), 2) AS imputation_rate,
    MIN(hour) AS first_price,
    MAX(hour) AS last_price,
    COUNT(DISTINCT DATE(hour)) AS days_with_data
FROM <blockchain_name>.price.ez_prices_hourly
WHERE hour >= CURRENT_DATE - 30
GROUP BY 1
HAVING COUNT(*) > 100
ORDER BY imputation_rate DESC;
```

{% enddocs %}

{% docs dim_asset_metadata_table_doc %}

## What

This table provides comprehensive metadata for all assets (tokens and native assets) tracked in the price schema across EVM blockchains. It includes provider, asset identifiers, names, symbols, contract addresses, blockchain, and verification status.

## Key Use Cases

- Join price data to asset metadata for enriched analysis
- Cross-chain asset analysis and mapping
- Asset discovery and verification status checking
- Provider-specific data source analysis

## Important Relationships

- **Join with ez_prices_hourly**: For price time series data
- **Join with core.dim_contracts**: For contract metadata

## Commonly-used Fields

- `provider`: Data source (coingecko, coinmarketcap, etc.)
- `asset_id`: Provider-specific unique identifier
- `blockchain`: Network identifier
- `token_address`: Contract address (NULL for native assets)
- `symbol`: Token ticker symbol
- `name`: Full asset name

## Sample Queries

**Basic Asset Lookup**
```sql
SELECT *
FROM <blockchain_name>.price.dim_asset_metadata
WHERE blockchain = 'ethereum'
ORDER BY symbol;
```

{% enddocs %}

{% docs fact_prices_ohlc_hourly_table_doc %}

## What

This table provides hourly OHLC (Open, High, Low, Close) price data for all assets tracked in the price schema. It is designed for time series analysis, volatility studies, and historical price lookups.

## Key Use Cases

- Technical analysis and candlestick chart creation
- Volatility studies and risk assessment
- Historical price lookups and trend analysis
- Market timing and trading analysis

## Important Relationships

- **Join with dim_asset_metadata**: For asset metadata
- **Join with ez_token_transfers**: For USD value calculations

## Commonly-used Fields

- `hour`: UTC timestamp for the price period
- `asset_id`: Unique identifier for the asset
- `open`: Opening price at start of hour
- `high`: Highest price during hour
- `low`: Lowest price during hour
- `close`: Closing price at end of hour

## Sample Queries

**OHLC Data Retrieval**
```sql
SELECT hour, asset_id, open, high, low, close
FROM <blockchain_name>.price.fact_prices_ohlc_hourly
WHERE asset_id = '<asset_id>'
  AND hour >= CURRENT_DATE - 30
ORDER BY hour DESC;
```

{% enddocs %}

{% docs ez_asset_metadata_table_doc %}

## What

This curated dimensional table provides comprehensive metadata for tokens and native assets across EVM blockchains. It serves as the authoritative source for asset information, with quality checks and verification status to ensure reliability.

## Key Use Cases

- Asset discovery and verification checking
- Cross-chain asset mapping and analysis
- Token metadata lookup for display purposes
- Filtering for verified or native assets only

## Important Relationships

- **Join with ez_prices_hourly**: For price data enrichment
- **Join with ez_token_transfers**: For transfer metadata

## Commonly-used Fields

- `token_address`: Contract address (NULL for native assets)
- `symbol`: Token ticker symbol
- `name`: Full asset name
- `decimals`: Token decimal places
- `is_verified`: Flipside verification status
- `is_native`: Native asset flag
- `blockchain`: Network identifier

## Sample Queries

**Verified Asset Discovery**
```sql
-- Find all verified USD stablecoins
SELECT 
    blockchain,
    token_address,
    name,
    symbol,
    decimals,
    is_verified
FROM <blockchain_name>.price.ez_asset_metadata
WHERE is_verified = TRUE
    AND (
        symbol IN ('USDC', 'USDT', 'DAI', 'BUSD')
        OR name ILIKE '%USD%'
        OR name ILIKE '%stablecoin%'
    )
    AND is_native = FALSE
ORDER BY blockchain, symbol;
```

**Cross-Chain Asset Mapping**
```sql
-- Find same assets across multiple chains
WITH asset_presence AS (
    SELECT 
        symbol,
        name,
        COUNT(DISTINCT blockchain) AS chain_count,
        ARRAY_AGG(DISTINCT blockchain) AS chains,
        ARRAY_AGG(token_address) AS addresses
    FROM <blockchain_name>.price.ez_asset_metadata
    WHERE is_native = FALSE
        AND is_verified = TRUE
    GROUP BY 1, 2
)
SELECT * FROM asset_presence
WHERE chain_count > 3
ORDER BY chain_count DESC, symbol;
```

**Native Asset Reference**
```sql
-- Get all native assets with metadata
SELECT 
    blockchain,
    symbol,
    name,
    decimals,
    CASE blockchain
        WHEN 'ethereum' THEN 'Proof of Stake'
        WHEN 'binance' THEN 'Proof of Staked Authority'
        WHEN 'polygon' THEN 'Proof of Stake'
        WHEN 'avalanche' THEN 'Avalanche Consensus'
        ELSE 'Various'
    END AS consensus_mechanism
FROM <blockchain_name>.price.ez_asset_metadata
WHERE is_native = TRUE
ORDER BY blockchain;
```

{% enddocs %}

{% docs ez_prices_address %}

Contract address of the token on the blockchain. NULL for native assets (ETH, AVAX, etc.).

Example: '0xa0b86a33e6776a1e7f9f0b8b8b8b8b8b8b8b8b8b'

{% enddocs %}

{% docs ez_prices_decimals %}

Number of decimal places for the token. Most ERC-20 tokens use 18 decimals, USDC/USDT use 6, WBTC uses 8.

Example: 18

{% enddocs %}

{% docs ez_prices_hour %}

UTC timestamp truncated to the hour for price recording. Used for joining with hourly transaction data.

Example: '2024-01-15 14:00:00.000'

{% enddocs %}

{% docs ez_prices_is_imputed %}

Boolean flag indicating if the price was forward-filled due to missing data. TRUE means price carried forward from last known value.

Example: false

{% enddocs %}

{% docs ez_prices_price %}

USD price of one whole token unit at the recorded hour.

Example: 3000.50

{% enddocs %}

{% docs ez_prices_symbol %}

Token ticker symbol as commonly recognized. Usually 3-5 uppercase characters.

Example: 'WETH'

{% enddocs %}

{% docs ez_prices_blockchain %}

The blockchain network where the asset exists. Lowercase by convention.

Example: 'ethereum'

{% enddocs %}

{% docs ez_prices_is_native %}

Boolean indicating if the asset is the blockchain's native currency. TRUE for ETH on Ethereum, AVAX on Avalanche, etc.

Example: true

{% enddocs %}

{% docs ez_prices_is_deprecated %}

Flag indicating if the asset is no longer actively supported. TRUE for deprecated assets that may have stale prices.

Example: false

{% enddocs %}

{% docs ez_prices_open %}

Opening price at the start of the hour in USD. First recorded price in the hour for OHLC analysis.

Example: 2995.25

{% enddocs %}

{% docs ez_prices_high %}

Highest price reached during the hour in USD. Maximum price in the hour for volatility analysis.

Example: 3005.75

{% enddocs %}

{% docs ez_prices_low %}

Lowest price reached during the hour in USD. Minimum price in the hour for support level analysis.

Example: 2985.50

{% enddocs %}

{% docs ez_prices_close %}

Closing price at the end of the hour in USD. Last recorded price in the hour, commonly used for valuations.

Example: 3000.50

{% enddocs %}

{% docs ez_prices_is_verified %}

Boolean indicating Flipside team verification of the asset. TRUE for manually verified assets with validated metadata.

Example: true

{% enddocs %}

{% docs ez_prices_provider %}

Data source that provided the price information. Values include 'coingecko', 'coinmarketcap', 'dex_aggregated'.

Example: 'coingecko'

{% enddocs %}

{% docs ez_prices_asset_id %}

Unique identifier for the asset from the price provider. Provider-specific ID format.

Example: 'ethereum'

{% enddocs %}

{% docs ez_prices_name %}

Full name of the asset or token. More descriptive than symbol.

Example: 'Wrapped Ether'

{% enddocs %}

{% docs ez_prices_blockchain_id %}

The numeric or string identifier for the blockchain on which the asset exists. Used for cross-chain mapping.

Example: '1'

{% enddocs %}