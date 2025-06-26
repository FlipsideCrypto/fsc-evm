{% docs ez_prices_hourly_table_doc %}

## Table: ez_prices_hourly

This curated table provides reliable hourly price data for tokens and native assets across EVM blockchains. It combines multiple data sources with quality checks to ensure accurate, consistent pricing for DeFi analytics, portfolio valuations, and historical analysis.

### Key Features:
- **Multi-Source Aggregation**: Prices from CoinGecko, CoinMarketCap, and DEX data
- **Quality Assured**: Outlier detection and validation rules applied
- **Complete Coverage**: Native assets and tokens in one table
- **Imputation Handling**: Forward-filled prices for low-liquidity assets
- **UTC Hourly Granularity**: Consistent hourly snapshots

### Data Sources Priority:
1. **DEX Prices**: High-volume on-chain trades (most accurate)
2. **CoinGecko**: Comprehensive coverage and reliability
3. **CoinMarketCap**: Additional coverage and validation
4. **Imputed Values**: Last known price for gaps

### Important Relationships:
- **Join with ez_token_transfers**: Calculate transfer USD values
- **Join with ez_asset_metadata**: Get token details and verification status
- **Join with dim_contracts**: Match addresses with contract metadata

### Sample Queries:

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

### Best Practices:
- **Check is_imputed**: Be aware of forward-filled prices for low liquidity
- **Use hourly joins**: Match DATE_TRUNC('hour', timestamp) for accuracy
- **Verify token addresses**: Ensure lowercase comparison
- **Handle NULLs**: Not all tokens have price data

{% enddocs %}

{% docs ez_asset_metadata_table_doc %}

## Table: ez_asset_metadata

This curated dimensional table provides comprehensive metadata for tokens and native assets across EVM blockchains. It serves as the authoritative source for asset information, with quality checks and verification status to ensure reliability.

### Key Features:
- **Unique Assets**: One row per token address per blockchain
- **Verified Data**: Flipside team verification for major assets
- **Complete Metadata**: Names, symbols, decimals, and categorization
- **Native Asset Support**: Includes blockchain native currencies
- **Cross-Chain Mapping**: Same asset across multiple chains

### Sample Queries:

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

Contract address of the token on the blockchain.

**Format**: Lowercase hex string (0x + 40 characters)
**NULL**: For native assets (ETH, AVAX, etc.)

{% enddocs %}

{% docs ez_prices_decimals %}

Number of decimal places for the token.

**Standard Values**:
- 18: Most ERC-20 tokens (ETH standard)
- 6: USDC, USDT
- 8: WBTC (Bitcoin standard)
- 0: Some NFT or special tokens

**Usage**: Required for converting raw amounts to human-readable values

{% enddocs %}

{% docs ez_prices_hour %}

UTC timestamp truncated to the hour for price recording.

**Format**: TIMESTAMP_NTZ rounded to hour
**Frequency**: Hourly snapshots
**Timezone**: Always UTC

**Join Pattern**:
```sql
-- Proper hourly join
JOIN <blockchain_name>.price.ez_prices_hourly p
ON DATE_TRUNC('hour', transaction_timestamp) = p.hour
```

{% enddocs %}

{% docs ez_prices_is_imputed %}

Boolean flag indicating if the price was forward-filled due to missing data.

**TRUE**: Price carried forward from last known value
**FALSE**: Actual price data from source

**Common Reasons**:
- Low liquidity tokens
- Exchange API downtime
- New token listings

**Analysis Consideration**:
```sql
-- Filter out imputed prices for volatility analysis
WHERE is_imputed = FALSE
```

{% enddocs %}

{% docs ez_prices_price %}

USD price of one whole token unit at the recorded hour.

**Format**: Decimal value
**Unit**: US Dollars per token
**Example**: For WETH at $3,000, price = 3000.00

{% enddocs %}

{% docs ez_prices_symbol %}

Token ticker symbol as commonly recognized.

**Examples**: ETH, WBTC, USDC, UNI
**Standards**: Usually 3-5 uppercase characters
**Note**: Not unique - multiple tokens may share symbols

{% enddocs %}

{% docs ez_prices_blockchain %}

The blockchain network where the asset exists.

**Values**: ethereum, polygon, avalanche, arbitrum, optimism, etc.
**Case**: Lowercase by convention
**Usage**: Required for multi-chain queries

{% enddocs %}

{% docs ez_prices_is_native %}

Boolean indicating if the asset is the blockchain's native currency.

**TRUE**: Native assets (ETH on Ethereum, AVAX on Avalanche)
**FALSE**: Token contracts (ERC-20, etc.)

**Query Usage**:
```sql
-- Get native asset prices only
WHERE is_native = TRUE
```

{% enddocs %}

{% docs ez_prices_is_deprecated %}

Flag indicating if the asset is no longer actively supported.

**TRUE**: Deprecated, may have stale prices
**FALSE**: Active asset with current data

**Common Deprecation Reasons**:
- Token migration to new contract
- Project closure
- Exchange delisting

{% enddocs %}

{% docs ez_prices_open %}

Opening price at the start of the hour in USD.

**OHLC Component**: First recorded price in the hour
**Usage**: For candlestick charts and technical analysis

{% enddocs %}

{% docs ez_prices_high %}

Highest price reached during the hour in USD.

**OHLC Component**: Maximum price in the hour
**Usage**: Volatility analysis and resistance levels

{% enddocs %}

{% docs ez_prices_low %}

Lowest price reached during the hour in USD.

**OHLC Component**: Minimum price in the hour
**Usage**: Volatility analysis and support levels

{% enddocs %}

{% docs ez_prices_close %}

Closing price at the end of the hour in USD.

**OHLC Component**: Last recorded price in the hour
**Usage**: Most commonly used for point-in-time valuations

**Note**: In ez_prices_hourly, typically close price is used as `price`

{% enddocs %}

{% docs ez_prices_is_verified %}

Boolean indicating Flipside team verification of the asset.

**TRUE**: Manually verified by Flipside team
**FALSE**: Unverified, use with caution

**Verification Includes**:
- Contract address validation
- Symbol/name accuracy
- Decimal precision
- No malicious behavior

**Best Practice**:
```sql
-- Prefer verified assets for production queries
WHERE is_verified = TRUE
```

{% enddocs %}

{% docs ez_prices_provider %}

Data source that provided the price information.

**Values**:
- coingecko: CoinGecko API
- coinmarketcap: CoinMarketCap API
- dex_aggregated: On-chain DEX prices

**Usage**: For data quality analysis and source preference

{% enddocs %}

{% docs ez_prices_asset_id %}

Unique identifier for the asset from the price provider.

**Format**: Provider-specific ID
**Examples**: 
- CoinGecko: 'ethereum', 'bitcoin'
- CoinMarketCap: Numeric IDs

**Usage**: For joining with provider-specific data

{% enddocs %}

{% docs ez_prices_name %}

Full name of the asset or token.

**Examples**: 
- "Ethereum"
- "USD Coin"
- "Wrapped Bitcoin"

**Note**: More descriptive than symbol

{% enddocs %}