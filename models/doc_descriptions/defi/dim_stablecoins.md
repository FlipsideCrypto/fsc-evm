{% docs dim_stablecoins_table_doc %}

## What

This table provides a dimensional view of verified stablecoins across EVM-compatible blockchains. It consolidates stablecoin metadata from various sources to create a unified reference table for identifying and analyzing stablecoin tokens.

## Key Use Cases

- Identifying stablecoin tokens in transaction and event data
- Filtering DeFi activities to stablecoin-only transactions
- Analyzing stablecoin adoption and distribution
- Tracking verified stablecoin contracts across chains
- Building stablecoin-specific metrics and dashboards

## Important Relationships

- **Join with defi.ez_stablecoins_supply**: Use `contract_address` for supply metrics

## Commonly-used Fields

- `contract_address`: Unique stablecoin token contract address
- `symbol`: Token symbol (e.g., USDC, USDT, DAI)
- `name`: Full token name
- `label`: Combined symbol and name, as a stablecoin unique identifier
- `decimals`: Number of decimal places for the token
- `is_verified`: Verification status

## Sample queries

```sql
-- Get unique stablecoins
SELECT 
    label AS stablecoin,
    COUNT(*) AS token_count
FROM <blockchain_name>.defi.dim_stablecoins
GROUP BY 1
ORDER BY 2 DESC;

-- Get all USDC variants
SELECT 
    contract_address,
    symbol,
    name,
    decimals
FROM <blockchain_name>.defi.dim_stablecoins
WHERE symbol LIKE '%USDC%'
ORDER BY symbol;

-- Check if specific address is a stablecoin
SELECT 
    contract_address,
    label,
    decimals
FROM <blockchain_name>.defi.dim_stablecoins
WHERE contract_address = LOWER('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48');
```

{% enddocs %}

{% docs dim_stablecoins_contract_address %}

The unique smart contract address of the stablecoin token.

Example: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'

{% enddocs %}

{% docs dim_stablecoins_symbol %}

The symbol identifier for the stablecoin token.

Example: 'USDC'

{% enddocs %}

{% docs dim_stablecoins_name %}

The full name of the stablecoin token.

Example: 'USD Coin'

{% enddocs %}

{% docs dim_stablecoins_label %}

A combined display label containing both symbol and name.

Example: 'USDC: USD Coin'

{% enddocs %}

{% docs dim_stablecoins_decimals %}

The number of decimal places used by the token contract.

Example: 6

{% enddocs %}

{% docs dim_stablecoins_is_verified %}

Indicates whether the stablecoin is verified by the Flipside team.

Example: true

{% enddocs %}

