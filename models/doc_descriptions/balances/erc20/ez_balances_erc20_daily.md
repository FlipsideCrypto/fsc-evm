{% docs ez_balances_erc20_daily_table_doc %}

## What

This table provides daily ERC20 token balance snapshots for verified token contracts by making direct `balanceOf` contract calls at the end of each day. **Important**: This table only includes a balance record for a given address and token on days when that address had transfer activity for that token. Balances are not rolled forward for every address-token pair every day. This means if an address holds a token but has no transfer activity on a given day, no balance record will be created for that day. This provides an efficient way to track token holdings across all verified ERC20 tokens with decimal adjustments and USD valuations where available. Historical ERC20 balances data available, starting from `2025-06-10`.

## Key Use Cases

- Daily portfolio tracking and balance monitoring for ERC20 tokens
- Historical balance analysis and trend identification
- Token holder distribution analysis at daily granularity
- Wallet balance snapshots for reporting and analytics
- Cross-token balance comparisons and concentration analysis
- Token supply distribution monitoring over time
- Daily balance-based yield and return calculations

## Important Relationships

- **Join with fact_blocks**: Use `block_number` for block metadata and timestamps
- **Join with dim_labels**: Use `address` for entity identification and categorization
- **Join with dim_contracts**: Use `contract_address` for token contract details
- **Join with ez_prices_hourly**: USD valuations already included but can be refreshed
- **Join with ez_balances_native_daily**: Compare with native token daily balances
- **Join with ez_token_transfers**: Compare daily balances with transfer activity

## Commonly-used Fields

- `address`: The account address holding the token balance
- `contract_address`: The ERC20 token contract address
- `symbol`: Token symbol (USDC, WETH, etc.)
- `balance`: Token balance at end of day, decimal adjusted to standard units
- `balance_usd`: USD value of the token balance at end of day
- `balance_raw`: Raw balance in smallest token unit (wei equivalent)
- `balance_precise`: Precise decimal-adjusted balance as string
- `decimals`: Number of decimal places for the token
- `block_date`: The date for which this balance snapshot was taken

## Sample queries

**Daily Token Holdings by Address**
```sql
SELECT 
    block_date,
    address,
    symbol,
    balance,
    balance_usd,
    contract_address
FROM <blockchain_name>.balances.ez_balances_erc20_daily
WHERE address = LOWER('0x1234567890123456789012345678901234567890')
    AND block_date >= CURRENT_DATE - 30
    AND balance > 0
ORDER BY block_date DESC, balance_usd DESC;
```

**Token Holder Count Trends**
```sql
SELECT 
    block_date,
    symbol,
    contract_address,
    COUNT(DISTINCT address) AS holder_count,
    SUM(balance) AS total_supply_tracked,
    AVG(balance) AS avg_balance,
    MEDIAN(balance) AS median_balance
FROM <blockchain_name>.balances.ez_balances_erc20_daily
WHERE block_date >= CURRENT_DATE - 90
    AND balance > 0
    AND symbol IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1 DESC, holder_count DESC;
```

**Portfolio Value Evolution**
```sql
-- Track portfolio value changes over time for specific addresses
SELECT 
    block_date,
    address,
    COUNT(DISTINCT contract_address) AS token_count,
    SUM(balance_usd) AS total_portfolio_usd,
    STRING_AGG(
        CASE WHEN balance_usd > 100 
        THEN symbol || ': $' || ROUND(balance_usd, 2) 
        END, ', '
    ) AS major_holdings
FROM <blockchain_name>.balances.ez_balances_erc20_daily
WHERE address IN (
    SELECT DISTINCT address 
    FROM <blockchain_name>.balances.ez_balances_erc20_daily 
    WHERE balance_usd > 10000
    LIMIT 100
)
    AND block_date >= CURRENT_DATE - 30
    AND balance > 0
GROUP BY 1, 2
HAVING total_portfolio_usd > 1000
ORDER BY 1 DESC, total_portfolio_usd DESC;
```

**Token Distribution Analysis**
```sql
-- Analyze token concentration and distribution patterns
SELECT 
    symbol,
    contract_address,
    block_date,
    COUNT(DISTINCT address) AS total_holders,
    COUNT(DISTINCT CASE WHEN balance >= 1000 THEN address END) AS holders_1k_plus,
    COUNT(DISTINCT CASE WHEN balance >= 10000 THEN address END) AS holders_10k_plus,
    MAX(balance) AS max_balance,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY balance) AS p95_balance,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY balance) AS median_balance
FROM <blockchain_name>.balances.ez_balances_erc20_daily
WHERE block_date = CURRENT_DATE - 1
    AND balance > 0
    AND symbol IS NOT NULL
GROUP BY 1, 2, 3
HAVING total_holders >= 100
ORDER BY total_holders DESC
LIMIT 50;
```

**Daily Balance Changes**
```sql
-- Compare daily balances to identify significant changes
WITH daily_changes AS (
    SELECT 
        address,
        contract_address,
        symbol,
        block_date,
        balance,
        balance_usd,
        LAG(balance) OVER (
            PARTITION BY address, contract_address 
            ORDER BY block_date
        ) AS prev_balance,
        LAG(balance_usd) OVER (
            PARTITION BY address, contract_address 
            ORDER BY block_date
        ) AS prev_balance_usd
    FROM <blockchain_name>.balances.ez_balances_erc20_daily
    WHERE block_date >= CURRENT_DATE - 7
        AND balance > 0
)
SELECT 
    block_date,
    address,
    symbol,
    balance,
    prev_balance,
    balance - prev_balance AS balance_change,
    balance_usd - prev_balance_usd AS balance_change_usd,
    CASE 
        WHEN prev_balance > 0 
        THEN ((balance - prev_balance) / prev_balance) * 100 
        ELSE NULL 
    END AS pct_change
FROM daily_changes
WHERE ABS(balance_change_usd) > 1000
    AND prev_balance IS NOT NULL
ORDER BY ABS(balance_change_usd) DESC
LIMIT 100;
```

{% enddocs %}

{% docs ez_balances_erc20_daily_block_date %}

The date for which this balance snapshot represents the end-of-day token balance.

Example: '2025-07-04'

{% enddocs %}

{% docs ez_balances_erc20_daily_address %}

The account address whose token balance is recorded in this daily snapshot.

Example: '0x1234567890123456789012345678901234567890'

{% enddocs %}

{% docs ez_balances_erc20_daily_contract_address %}

The ERC20 token contract address for which the balance is recorded.

Example: '0xa0b86a33e6eb88b4d81b15e4e60c8a5b776e3b7a'

{% enddocs %}

{% docs ez_balances_erc20_daily_decimals %}

Number of decimal places for the token, used for proper decimal adjustment in balance calculations.

Example: 6

{% enddocs %}

{% docs ez_balances_erc20_daily_symbol %}

The token symbol for the ERC20 token.

Example: 'USDC'

{% enddocs %}

{% docs ez_balances_erc20_daily_balance_hex %}

Hexadecimal representation of the token balance as returned by the balanceOf contract call.

Example: '0x3b9aca00'

{% enddocs %}

{% docs ez_balances_erc20_daily_balance_raw %}

Token balance in the smallest unit (wei equivalent) without decimal adjustment, as returned by the contract.

Example: 1000000000

{% enddocs %}

{% docs ez_balances_erc20_daily_balance_precise %}

Token balance with proper decimal adjustment, returned as a string to preserve precision.

Example: '1000.000000'

{% enddocs %}

{% docs ez_balances_erc20_daily_balance %}

Token balance with decimal adjustment converted to a float for easier mathematical operations.

Example: 1000.0

{% enddocs %}

{% docs ez_balances_erc20_daily_balance_usd %}

USD value of the token balance at the end of the day, calculated using hourly price data.

Example: 1000.50

{% enddocs %}