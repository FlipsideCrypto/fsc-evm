{% docs ez_lending_mcp_context %}

# Lending Tables MCP Context

## Table Overview
- **EZ_LENDING_BORROWS**: User borrowing transactions across lending protocols
- **EZ_LENDING_DEPOSITS**: User deposit transactions (collateral provision)
- **EZ_LENDING_WITHDRAWS**: User withdrawal transactions (collateral removal)
- **EZ_LENDING_REPAYMENTS**: User repayment transactions (debt settlement)
- **EZ_LENDING_LIQUIDATIONS**: Liquidation events (collateral seizure for debt)
- **EZ_LENDING_FLASHLOANS**: Flash loan transactions (borrow-repay in single tx)

## Critical Join Relationships

### Valid Token-Based Joins
- `deposits.token_address` ↔ `withdraws.token_address` (same collateral asset)
- `borrows.token_address` ↔ `repayments.token_address` (same borrowed asset)

### Invalid Token-Based Joins
- `borrows.token_address` ↔ `deposits.token_address` (borrowed ≠ collateral)
- `borrows.token_address` ↔ `withdraws.token_address` (borrowed ≠ collateral)

### User-Based Joins
- `deposits.depositor` ↔ `withdraws.depositor` (same user)
- `borrows.borrower` ↔ `repayments.borrower` (same user)
- `liquidations.borrower` ↔ `borrows.borrower` (liquidated user)
- `liquidations.liquidator` ↔ `flashloans.initiator` (liquidator activity)

## Column Mappings

### User Identifiers
| Table | User Column | Description |
|-------|-------------|-------------|
| EZ_LENDING_BORROWS | `borrower` | Address that borrowed assets |
| EZ_LENDING_DEPOSITS | `depositor` | Address that provided collateral |
| EZ_LENDING_WITHDRAWS | `depositor` | Address that withdrew collateral |
| EZ_LENDING_REPAYMENTS | `borrower` | Address that repaid debt |
| EZ_LENDING_LIQUIDATIONS | `borrower` | Address that was liquidated |
| EZ_LENDING_LIQUIDATIONS | `liquidator` | Address that performed liquidation |
| EZ_LENDING_FLASHLOANS | `initiator` | Address that initiated flash loan |

### Asset Identifiers
| Table | Token Columns | Description |
|-------|--------------|-------------|
| EZ_LENDING_BORROWS | `token_address`, `token_symbol` | Borrowed asset |
| EZ_LENDING_DEPOSITS | `token_address`, `token_symbol` | Collateral asset |
| EZ_LENDING_WITHDRAWS | `token_address`, `token_symbol` | Collateral asset |
| EZ_LENDING_REPAYMENTS | `token_address`, `token_symbol` | Repaid asset |
| EZ_LENDING_LIQUIDATIONS | `collateral_token`, `collateral_token_symbol` | Seized collateral |
| EZ_LENDING_LIQUIDATIONS | `debt_token`, `debt_token_symbol` | Covered debt |
| EZ_LENDING_FLASHLOANS | `flashloan_token_address`, `flashloan_token_symbol` | Flash borrowed asset |

### Amount Columns
| Table | Amount Columns | Description |
|-------|---------------|-------------|
| EZ_LENDING_BORROWS | `amount`, `amount_usd` | Borrowed quantity and USD value |
| EZ_LENDING_DEPOSITS | `amount`, `amount_usd` | Deposited quantity and USD value |
| EZ_LENDING_WITHDRAWS | `amount`, `amount_usd` | Withdrawn quantity and USD value |
| EZ_LENDING_REPAYMENTS | `amount`, `amount_usd` | Repaid quantity and USD value |
| EZ_LENDING_LIQUIDATIONS | `amount`, `amount_usd` | Liquidated collateral quantity and USD value |
| EZ_LENDING_FLASHLOANS | `flashloan_amount`, `flashloan_amount_usd` | Flash borrowed quantity and USD value |

### Protocol & Transaction Data
| Column | Tables | Description |
|--------|--------|-------------|
| `platform` | All tables | Lending protocol (aave, compound, etc.) |
| `block_timestamp` | All tables | Transaction timestamp |
| `block_number` | All tables | Block number |
| `tx_hash` | All tables | Transaction hash |
| `event_index` | All tables | Event index within transaction |

## Business Logic Rules

### Lending Flow
1. **Deposit**: User provides collateral (`deposits` table)
2. **Borrow**: User borrows against collateral (`borrows` table)
3. **Repay**: User repays borrowed amount (`repayments` table)
4. **Withdraw**: User withdraws collateral (`withdraws` table)

### Liquidation Triggers
- Occurs when collateral value falls below required threshold
- `liquidations` table captures collateral seizure events
- Liquidator receives collateral at discount

### Flash Loans
- Borrow and repay in single transaction
- Requires fee payment (`premium_amount`, `premium_amount_usd`)
- Used for arbitrage, debt refinancing, or liquidation

## Data Quality Notes

### Null Handling
- `amount_usd` may be NULL for tokens without price data
- `event_index` may be NULL for trace-based transactions
- Always use `COALESCE(amount_usd, 0)` in aggregations

### Performance Considerations
- Always filter by `block_timestamp` for large queries
- Index on user columns (`borrower`, `depositor`) for user analysis
- Use `token_address` for asset-specific queries

### Common Platforms
- aave, compound, maker, venus, benqi, moonwell
- Platform names are lowercase and standardized

## Analysis Patterns

### User Position Analysis
- Join deposits ↔ withdrawals on `depositor` + `token_address`
- Use `amount` (not `amount_usd`) for yield calculations
- Track net position: deposits - withdrawals

### Protocol Health Analysis
- Monitor deposit/withdrawal ratios
- Track liquidation frequency and size
- Analyze flash loan usage patterns

### Risk Analysis
- Large withdrawals may indicate protocol stress
- High liquidation rates suggest market volatility
- Flash loan spikes may indicate arbitrage opportunities

{% enddocs %}

{% docs ez_lending_borrows_table_doc %}

## What

This table provides a comprehensive view of borrowing transactions across all major lending protocols on EVM blockchains. It captures when users borrow assets against their deposited collateral, enabling analysis of lending market dynamics, user behavior, and protocol performance.

## Key Use Cases

- Tracking borrowing volumes and user activity across protocols
- Analyzing most borrowed assets and their trends
- Understanding user borrowing patterns and behavior
- Monitoring protocol market share and growth
- Calculating outstanding loan positions

## Important Relationships

- Links to `ez_lending_deposits` for collateral analysis
- Joins with `ez_lending_repayments` to track loan lifecycle
- References `ez_lending_liquidations` for risk analysis
- Connects to `price.ez_prices_hourly` for USD valuations

## Commonly-used Fields

- `borrower`: Address that borrowed assets
- `platform`: Lending protocol name
- `token_address`/`token_symbol`: Borrowed asset details
- `amount`/`amount_usd`: Borrowed quantity and USD value
- `block_timestamp`: When borrow occurred

## Sample queries

```sql
-- Daily borrowing volume by protocol
SELECT 
    DATE_TRUNC('day', block_timestamp) AS date,
    platform,
    COUNT(DISTINCT tx_hash) AS borrow_txns,
    COUNT(DISTINCT borrower) AS unique_borrowers,
    SUM(amount_usd) AS volume_usd
FROM <blockchain_name>.defi.ez_lending_borrows
WHERE block_timestamp >= CURRENT_DATE - 30
    AND amount_usd IS NOT NULL
GROUP BY 1, 2
ORDER BY 1 DESC, 5 DESC;

-- Top borrowed assets analysis
SELECT 
    token_symbol,
    token_address,
    COUNT(*) AS borrow_count,
    SUM(amount) AS total_borrowed,
    SUM(amount_usd) AS total_borrowed_usd,
    AVG(amount_usd) AS avg_borrow_size_usd
FROM <blockchain_name>.defi.ez_lending_borrows
WHERE block_timestamp >= CURRENT_DATE - 7
    AND token_symbol IS NOT NULL
GROUP BY 1, 2
ORDER BY 5 DESC
LIMIT 20;

-- Wallet Specific Borrow Analysis
SELECT 
    b.borrower,
    b.token_address AS borrowed_token_address,
    b.token_symbol AS borrowed_token_symbol,
    DATE_TRUNC('week', b.block_timestamp) AS weekly_block_timestamp,
    SUM(b.amount) AS total_borrow_amount,
    SUM(b.amount_usd) AS total_borrow_usd,
    SUM(r.amount) AS total_repayment_amount,
    SUM(r.amount_usd) AS total_repayment_usd,
    SUM(b.amount) - SUM(r.amount) AS net_borrowed_amount,
    SUM(b.amount_usd) - SUM(r.amount_usd) AS net_borrowed_usd
FROM 
    <blockchain_name>.defi.ez_lending_borrows b
LEFT JOIN <blockchain_name>.defi.ez_lending_repayments r
    ON b.borrower = r.borrower
    AND b.token_address = r.token_address
WHERE 
    b.borrower = LOWER('<user_address>')
GROUP BY 1, 2, 3, 4

-- User borrowing patterns
WITH user_stats AS (
    SELECT 
        borrower,
        COUNT(DISTINCT DATE_TRUNC('day', block_timestamp)) AS active_days,
        COUNT(DISTINCT platform) AS platforms_used,
        COUNT(DISTINCT token_address) AS assets_borrowed,
        SUM(amount_usd) AS total_borrowed_usd,
        AVG(amount_usd) AS avg_borrow_size
    FROM <blockchain_name>.defi.ez_lending_borrows
    WHERE block_timestamp >= CURRENT_DATE - 30
        AND amount_usd IS NOT NULL
    GROUP BY 1
)
SELECT 
    CASE 
        WHEN total_borrowed_usd < 1000 THEN '< $1K'
        WHEN total_borrowed_usd < 10000 THEN '$1K - $10K'
        WHEN total_borrowed_usd < 100000 THEN '$10K - $100K'
        ELSE '> $100K'
    END AS borrower_tier,
    COUNT(*) AS user_count,
    AVG(active_days) AS avg_active_days,
    AVG(platforms_used) AS avg_platforms,
    AVG(total_borrowed_usd) AS avg_total_borrowed
FROM user_stats
GROUP BY 1
ORDER BY 5 DESC;

-- Protocol market share
WITH protocol_volume AS (
    SELECT 
        platform,
        SUM(amount_usd) AS total_volume,
        COUNT(DISTINCT borrower) AS unique_users,
        COUNT(*) AS transaction_count
    FROM <blockchain_name>.defi.ez_lending_borrows
    WHERE block_timestamp >= CURRENT_DATE - 30
        AND amount_usd IS NOT NULL
    GROUP BY 1
)
SELECT 
    platform,
    total_volume,
    total_volume * 100.0 / SUM(total_volume) OVER () AS market_share_pct,
    unique_users,
    transaction_count,
    total_volume / transaction_count AS avg_borrow_size
FROM protocol_volume
ORDER BY total_volume DESC;
```

{% enddocs %}

{% docs ez_lending_deposits_table_doc %}

## What

This table tracks all deposit transactions across lending protocols on EVM blockchains. Deposits represent users supplying liquidity to lending pools, earning yield while enabling their assets to serve as collateral for borrowing.

## Key Use Cases

- Calculating total value locked (TVL) by protocol
- Analyzing deposit and withdrawal patterns
- Tracking user liquidity provision behavior
- Monitoring asset distribution across protocols
- Identifying whale depositor activity

## Important Relationships

- Links to `ez_lending_borrows` for collateralization analysis
- Joins with `ez_lending_withdraws` to track position lifecycle
- References protocol-specific token contracts (aTokens, cTokens, etc.)
- Connects to `price.ez_prices_hourly` for USD valuations

## Commonly-used Fields

- `depositor`: Address supplying liquidity
- `platform`: Lending protocol name
- `token_address`/`token_symbol`: Deposited asset details
- `amount`/`amount_usd`: Deposit quantity and USD value
- `block_timestamp`: When deposit occurred

## Sample queries

```sql
-- Daily deposit volume and TVL calculation
WITH daily_metrics AS (
    SELECT 
        DATE_TRUNC('day', block_timestamp) AS date,
        platform,
        SUM(amount_usd) AS daily_deposits_usd,
        COUNT(DISTINCT depositor) AS unique_depositors
    FROM <blockchain_name>.defi.ez_lending_deposits
    WHERE block_timestamp >= CURRENT_DATE - 30
        AND amount_usd IS NOT NULL
    GROUP BY 1, 2
),
daily_withdrawals AS (
    SELECT 
        DATE_TRUNC('day', block_timestamp) AS date,
        platform,
        SUM(amount_usd) AS daily_withdrawals_usd
    FROM <blockchain_name>.defi.ez_lending_withdraws
    WHERE block_timestamp >= CURRENT_DATE - 30
        AND amount_usd IS NOT NULL
    GROUP BY 1, 2
)
SELECT 
    m.date,
    m.platform,
    m.daily_deposits_usd,
    COALESCE(w.daily_withdrawals_usd, 0) AS daily_withdrawals_usd,
    m.daily_deposits_usd - COALESCE(w.daily_withdrawals_usd, 0) AS net_flow_usd,
    SUM(m.daily_deposits_usd - COALESCE(w.daily_withdrawals_usd, 0)) 
        OVER (PARTITION BY m.platform ORDER BY m.date) AS cumulative_tvl_estimate
FROM daily_metrics m
LEFT JOIN daily_withdrawals w ON m.date = w.date AND m.platform = w.platform
ORDER BY m.date DESC, m.platform;

-- Wallet level deposit and withdraw analysis
SELECT 
    d.depositor,
    d.token_address AS collateral_token_address,
    d.token_symbol AS collateral_token_symbol,
    DATE_TRUNC('week', d.block_timestamp) AS weekly_block_timestamp,
    SUM(d.amount) AS total_deposit_amount,
    SUM(d.amount_usd) AS total_deposit_usd,
    SUM(w.amount) AS total_withdraw_amount,
    SUM(w.amount_usd) AS total_withdraw_usd,
    SUM(d.amount) - SUM(w.amount) AS net_collateral_amount,
    SUM(d.amount_usd) - SUM(w.amount_usd) AS net_collateral_usd
FROM 
    <blockchain_name>.defi.ez_lending_deposits d
LEFT JOIN <blockchain_name>.defi.ez_lending_withdraws w
    ON d.depositor = w.depositor
    AND d.token_address = w.token_address
WHERE 
    d.depositor = LOWER('<user_address>')
GROUP BY 1, 2, 3, 4;

-- Depositor behavior analysis
WITH depositor_activity AS (
    SELECT 
        depositor,
        COUNT(DISTINCT platform) AS platforms_used,
        COUNT(DISTINCT token_address) AS unique_assets,
        SUM(amount_usd) AS total_deposited_usd,
        MAX(block_timestamp) AS last_deposit,
        MIN(block_timestamp) AS first_deposit
    FROM <blockchain_name>.defi.ez_lending_deposits
    WHERE amount_usd IS NOT NULL
    GROUP BY 1
)
SELECT 
    CASE 
        WHEN platforms_used = 1 THEN 'Single Protocol'
        WHEN platforms_used = 2 THEN 'Two Protocols'
        ELSE 'Multi-Protocol'
    END AS user_type,
    COUNT(*) AS user_count,
    AVG(total_deposited_usd) AS avg_deposit_size,
    AVG(unique_assets) AS avg_assets_deposited,
    AVG(DATEDIFF('day', first_deposit, last_deposit)) AS avg_active_days
FROM depositor_activity
GROUP BY 1
ORDER BY 2 DESC;

-- Asset distribution by protocol
SELECT 
    platform,
    token_symbol,
    COUNT(*) AS deposit_transactions,
    SUM(amount) AS total_amount,
    SUM(amount_usd) AS total_usd,
    SUM(amount_usd) * 100.0 / SUM(SUM(amount_usd)) OVER (PARTITION BY platform) AS pct_of_protocol
FROM <blockchain_name>.defi.ez_lending_deposits
WHERE block_timestamp >= CURRENT_DATE - 30
    AND amount_usd IS NOT NULL
    AND token_symbol IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 6 DESC;

-- Large deposits monitoring (whale activity)
SELECT 
    block_timestamp,
    tx_hash,
    platform,
    depositor,
    token_symbol,
    amount,
    amount_usd
FROM <blockchain_name>.defi.ez_lending_deposits
WHERE amount_usd > 1000000
    AND block_timestamp >= CURRENT_DATE - 7
ORDER BY amount_usd DESC;
```

{% enddocs %}

{% docs ez_lending_flashloans_table_doc %}

## What

This table captures flash loan transactions across lending protocols. Flash loans enable borrowing without collateral within a single transaction, provided the loan plus fees are repaid before transaction completion. This advanced DeFi primitive is primarily used for arbitrage, collateral swapping, and liquidations.

## Key Use Cases

- Analyzing arbitrage and MEV activity patterns
- Tracking flash loan volume and fee revenue
- Identifying power users and bot activity
- Monitoring large-scale DeFi operations
- Understanding cross-protocol composability

## Important Relationships

- Often precedes transactions in DEX tables for arbitrage analysis
- Links to `ez_lending_liquidations` for liquidation strategies
- May connect to multiple protocols within single transaction
- References `price.ez_prices_hourly` for USD valuations

## Commonly-used Fields

- `initiator`: Address that triggered the flash loan
- `platform`: Lending protocol providing the loan
- `flashloan_token`/`flashloan_token_symbol`: Borrowed asset
- `flashloan_amount`/`flashloan_amount_usd`: Loan size
- `premium_amount`/`premium_amount_usd`: Fee paid

## Sample queries

```sql
-- Daily flash loan volume and fees
SELECT 
    DATE_TRUNC('day', block_timestamp) AS date,
    platform,
    COUNT(*) AS flashloan_count,
    SUM(flashloan_amount_usd) AS total_volume_usd,
    SUM(premium_amount_usd) AS total_fees_usd,
    AVG(premium_amount_usd / NULLIF(flashloan_amount_usd, 0) * 100) AS avg_fee_rate_pct
FROM <blockchain_name>.defi.ez_lending_flashloans
WHERE block_timestamp >= CURRENT_DATE - 30
    AND flashloan_amount_usd IS NOT NULL
GROUP BY 1, 2
ORDER BY 1 DESC, 4 DESC;

-- Most flash loaned assets
SELECT 
    flashloan_token_symbol,
    flashloan_token,
    COUNT(*) AS loan_count,
    SUM(flashloan_amount) AS total_amount,
    SUM(flashloan_amount_usd) AS total_volume_usd,
    AVG(flashloan_amount_usd) AS avg_loan_size_usd,
    SUM(premium_amount_usd) AS total_fees_collected
FROM <blockchain_name>.defi.ez_lending_flashloans
WHERE block_timestamp >= CURRENT_DATE - 7
    AND flashloan_token_symbol IS NOT NULL
GROUP BY 1, 2
ORDER BY 5 DESC
LIMIT 20;

-- Flash loan user analysis
WITH flashloan_users AS (
    SELECT 
        initiator,
        COUNT(*) AS flashloan_count,
        COUNT(DISTINCT DATE_TRUNC('day', block_timestamp)) AS active_days,
        COUNT(DISTINCT platform) AS protocols_used,
        SUM(flashloan_amount_usd) AS total_borrowed_usd,
        SUM(premium_amount_usd) AS total_fees_paid_usd
    FROM <blockchain_name>.defi.ez_lending_flashloans
    WHERE block_timestamp >= CURRENT_DATE - 30
        AND flashloan_amount_usd IS NOT NULL
    GROUP BY 1
)
SELECT 
    CASE 
        WHEN flashloan_count = 1 THEN 'One-time User'
        WHEN flashloan_count <= 10 THEN 'Occasional User'
        WHEN flashloan_count <= 100 THEN 'Regular User'
        ELSE 'Power User'
    END AS user_category,
    COUNT(*) AS user_count,
    SUM(total_borrowed_usd) AS category_volume_usd,
    AVG(total_fees_paid_usd) AS avg_fees_per_user
FROM flashloan_users
GROUP BY 1
ORDER BY 3 DESC;

-- Large flash loans (potential arbitrage/liquidations)
SELECT 
    block_timestamp,
    tx_hash,
    platform,
    initiator,
    target,
    flashloan_token_symbol,
    flashloan_amount_usd,
    premium_amount_usd,
    premium_amount_usd / NULLIF(flashloan_amount_usd, 0) * 100 AS fee_rate_pct
FROM <blockchain_name>.defi.ez_lending_flashloans
WHERE flashloan_amount_usd > 1000000
    AND block_timestamp >= CURRENT_DATE - 1
ORDER BY flashloan_amount_usd DESC;

-- Wallet-specific flash loan analysis
SELECT 
    initiator,
    platform,
    flashloan_token_symbol,
    COUNT(*) AS flashloan_count,
    SUM(flashloan_amount_usd) AS total_borrowed_usd,
    SUM(premium_amount_usd) AS total_fees_paid_usd,
    AVG(premium_amount_usd / NULLIF(flashloan_amount_usd, 0) * 100) AS avg_fee_rate_pct,
    MIN(block_timestamp) AS first_flashloan,
    MAX(block_timestamp) AS last_flashloan,
    COUNT(DISTINCT DATE_TRUNC('day', block_timestamp)) AS active_days
FROM <blockchain_name>.defi.ez_lending_flashloans
WHERE initiator = LOWER('<wallet_address>')
    AND block_timestamp >= CURRENT_DATE - 30
    AND flashloan_amount_usd IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY total_borrowed_usd DESC;
```

{% enddocs %}

{% docs ez_lending_liquidations_table_doc %}

## What

This table tracks liquidation events across lending protocols, where under-collateralized positions are forcibly closed to protect protocol solvency. Liquidations occur when a borrower's health factor drops below 1, typically due to collateral value decline or debt value increase.

## Key Use Cases

- Monitoring protocol health and risk levels
- Analyzing liquidation patterns during market volatility
- Tracking liquidator profitability and competition
- Understanding collateral risk profiles
- Identifying frequently liquidated borrowers

## Important Relationships

- Links to `ez_lending_borrows` for original loan details
- Connects to `ez_lending_deposits` for collateral information
- Often preceded by entries in `ez_lending_flashloans`
- References `price.ez_prices_hourly` for USD valuations

## Commonly-used Fields

- `borrower`: Address that was liquidated
- `liquidator`: Address performing the liquidation
- `platform`: Lending protocol
- `collateral_token`/`debt_token`: Asset pair involved
- `amount`/`amount_usd`: Collateral seized

## Sample queries

```sql
-- Daily liquidation volume and metrics
SELECT 
    DATE_TRUNC('day', block_timestamp) AS date,
    platform,
    COUNT(*) AS liquidation_count,
    COUNT(DISTINCT borrower) AS unique_borrowers_liquidated,
    SUM(amount_usd) AS total_debt_covered_usd,
    SUM(amount_usd) AS total_collateral_liquidated_usd,
    AVG(amount_usd / NULLIF(amount_usd, 0) - 1) * 100 AS avg_liquidation_bonus_pct
FROM <blockchain_name>.defi.ez_lending_liquidations
WHERE block_timestamp >= CURRENT_DATE - 30
    AND amount_usd IS NOT NULL
GROUP BY 1, 2
ORDER BY 1 DESC, 5 DESC;

-- Most liquidated asset pairs
SELECT 
    collateral_token,
    collateral_token_symbol,
    debt_token,
    debt_token_symbol,
    COUNT(*) AS liquidation_count,
    SUM(amount_usd) AS total_collateral_liquidated_usd,
    AVG(amount_usd) AS avg_liquidation_size_usd
FROM <blockchain_name>.defi.ez_lending_liquidations
WHERE block_timestamp >= CURRENT_DATE - 7
    AND collateral_token_symbol IS NOT NULL
    AND debt_token_symbol IS NOT NULL
GROUP BY 1, 2, 3, 4
ORDER BY 6 DESC
LIMIT 20;

-- Liquidator analysis
WITH liquidator_stats AS (
    SELECT 
        liquidator,
        COUNT(*) AS liquidations_performed,
        SUM(amount_usd) AS total_collateral_received_usd,
    FROM <blockchain_name>.defi.ez_lending_liquidations
    WHERE block_timestamp >= CURRENT_DATE - 30
        AND amount_usd IS NOT NULL
    GROUP BY 1
)
SELECT 
    CASE 
        WHEN liquidations_performed = 1 THEN 'Opportunistic'
        WHEN liquidations_performed <= 10 THEN 'Active'
        WHEN liquidations_performed <= 100 THEN 'Professional'
        ELSE 'Bot/High Frequency'
    END AS liquidator_type,
    COUNT(*) AS liquidator_count,
    SUM(total_collateral_received_usd) as total_collateral_received_usd
    SUM(liquidations_performed) AS total_liquidations
FROM liquidator_stats
GROUP BY 1
ORDER BY 3 DESC;

-- Large liquidations monitoring
SELECT 
    block_timestamp,
    tx_hash,
    platform,
    borrower,
    liquidator,
    collateral_token_symbol,
    debt_token_symbol,
    amount_usd,
FROM <blockchain_name>.defi.ez_lending_liquidations
WHERE amount_usd > 10000
    AND block_timestamp >= CURRENT_DATE - 14
ORDER BY amount_usd DESC;

-- Borrower liquidation history
WITH borrower_liquidations AS (
    SELECT 
        borrower,
        COUNT(*) AS times_liquidated,
        COUNT(DISTINCT DATE_TRUNC('day', block_timestamp)) AS liquidation_days,
        SUM(amount_usd) AS total_collateral_lost_usd,
        ARRAY_AGG(DISTINCT platform) AS platforms_liquidated_on
    FROM <blockchain_name>.defi.ez_lending_liquidations
    WHERE block_timestamp >= CURRENT_DATE - 90
        AND amount_usd IS NOT NULL
    GROUP BY 1
)
SELECT 
    times_liquidated,
    platforms_liquidated_on,
    COUNT(*) AS borrower_count,
    AVG(total_collateral_lost_usd) AS avg_loss_from_liquidation
FROM borrower_liquidations
GROUP BY 1, 2
ORDER BY 1, 2;
```

{% enddocs %}

{% docs ez_lending_repayments_table_doc %}

## What

This table contains loan repayment transactions across lending protocols. Repayments reduce or eliminate outstanding debt positions, with amounts including both principal and accrued interest. Understanding repayment patterns helps analyze user behavior and protocol health.

## Key Use Cases

- Tracking loan lifecycle and duration analysis
- Calculating interest paid on borrowed positions
- Understanding repayment patterns by user segment
- Monitoring protocol revenue from interest
- Analyzing third-party repayment activity

## Important Relationships

- Links to `ez_lending_borrows` for original loan details
- Connects to `ez_lending_liquidations` (forced repayments)
- May reference `ez_lending_deposits` for collateral release
- Uses `price.ez_prices_hourly` for USD valuations

## Commonly-used Fields

- `borrower`: Address with the loan
- `payer`: Address making the payment (may differ)
- `platform`: Lending protocol
- `token_address`/`token_symbol`: Repaid asset
- `amount`/`amount_usd`: Repayment quantity

## Sample queries

```sql
-- Daily repayment volume and metrics
SELECT 
    DATE_TRUNC('day', block_timestamp) AS date,
    platform,
    COUNT(*) AS repayment_count,
    COUNT(DISTINCT borrower) AS unique_borrowers,
    SUM(amount_usd) AS total_repaid_usd,
    AVG(amount_usd) AS avg_repayment_size_usd
FROM <blockchain_name>.defi.ez_lending_repayments
WHERE block_timestamp >= CURRENT_DATE - 30
    AND amount_usd IS NOT NULL
GROUP BY 1, 2
ORDER BY 1 DESC, 5 DESC;

-- Loan duration analysis
WITH loan_lifecycles AS (
    SELECT 
        b.borrower,
        b.platform,
        b.token_symbol,
        b.block_timestamp AS borrow_time,
        MIN(r.block_timestamp) AS first_repayment_time,
        SUM(b.amount_usd) AS borrowed_usd,
        SUM(r.amount_usd) AS total_repaid_usd
    FROM <blockchain_name>.defi.ez_lending_borrows b
    INNER JOIN <blockchain_name>.defi.ez_lending_repayments r
        ON b.borrower = r.borrower
        AND b.platform = r.platform
        AND b.token_address = r.token_address
        AND r.block_timestamp > b.block_timestamp
    WHERE b.block_timestamp >= CURRENT_DATE - 90
        AND b.amount_usd IS NOT NULL
        AND r.amount_usd IS NOT NULL
    GROUP BY 1, 2, 3, 4
)
SELECT 
    platform,
    token_symbol,
    AVG(DATEDIFF('day', borrow_time, first_repayment_time)) AS avg_days_to_first_repayment,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DATEDIFF('day', borrow_time, first_repayment_time)) AS median_days,
    COUNT(*) AS loan_count,
    AVG(total_repaid_usd / NULLIF(borrowed_usd, 0) - 1) * 100 AS avg_interest_paid_pct
FROM loan_lifecycles
GROUP BY 1, 2
HAVING COUNT(*) > 10
ORDER BY 3;

-- Repayment patterns by user segment
WITH user_repayment_stats AS (
    SELECT 
        borrower,
        COUNT(*) AS repayment_count,
        SUM(amount_usd) AS total_repaid_usd,
        AVG(amount_usd) AS avg_repayment_size,
        COUNT(DISTINCT DATE_TRUNC('month', block_timestamp)) AS active_months,
        COUNT(DISTINCT token_address) AS unique_assets_repaid
    FROM <blockchain_name>.defi.ez_lending_repayments
    WHERE block_timestamp >= CURRENT_DATE - 180
        AND amount_usd IS NOT NULL
    GROUP BY 1
)
SELECT 
    CASE 
        WHEN total_repaid_usd < 1000 THEN 'Micro (<$1K)'
        WHEN total_repaid_usd < 10000 THEN 'Small ($1K-$10K)'
        WHEN total_repaid_usd < 100000 THEN 'Medium ($10K-$100K)'
        WHEN total_repaid_usd < 1000000 THEN 'Large ($100K-$1M)'
        ELSE 'Whale (>$1M)'
    END AS borrower_segment,
    COUNT(*) AS borrower_count,
    AVG(repayment_count) AS avg_repayments_per_user,
    AVG(avg_repayment_size) AS avg_repayment_size,
    AVG(active_months) AS avg_active_months
FROM user_repayment_stats
GROUP BY 1
ORDER BY 2 DESC;

-- Asset-specific repayment velocity
SELECT 
    token_symbol,
    platform,
    COUNT(*) AS repayment_transactions,
    COUNT(DISTINCT borrower) AS unique_repayers,
    SUM(amount_usd) AS total_usd_repaid,
    AVG(amount_usd) AS avg_repayment_usd,
    SUM(amount_usd) / COUNT(DISTINCT DATE_TRUNC('day', block_timestamp)) AS daily_velocity_usd
FROM <blockchain_name>.defi.ez_lending_repayments
WHERE block_timestamp >= CURRENT_DATE - 30
    AND token_symbol IS NOT NULL
    AND amount_usd IS NOT NULL
GROUP BY 1, 2
HAVING COUNT(*) > 50
ORDER BY 8 DESC;

-- Large repayments monitoring
SELECT 
    block_timestamp,
    tx_hash,
    platform,
    borrower,
    payer,
    token_symbol,
    amount_usd,
    CASE WHEN borrower = payer THEN 'Self' ELSE 'Third-party' END AS repayment_type
FROM <blockchain_name>.defi.ez_lending_repayments
WHERE amount_usd > 500000
    AND block_timestamp >= CURRENT_DATE - 7
ORDER BY amount_usd DESC;
```

{% enddocs %}

{% docs ez_lending_withdraws_table_doc %}

## What

This table tracks withdrawal transactions where users remove their supplied liquidity from lending protocols. Withdrawals include the original deposit plus earned interest, subject to available liquidity in the protocol.

## Key Use Cases

- Monitoring liquidity flows and protocol health
- Detecting potential bank run scenarios
- Calculating realized yields for depositors
- Analyzing withdrawal patterns and timing
- Tracking large withdrawals that may impact rates

## Important Relationships

- Links to `ez_lending_deposits` for position lifecycle
- Affected by `ez_lending_borrows` (reduces available liquidity)
- Increased by `ez_lending_repayments` (adds liquidity)
- References `price.ez_prices_hourly` for USD valuations

## Commonly-used Fields

- `depositor`: Address withdrawing funds
- `platform`: Lending protocol
- `token_address`/`token_symbol`: Withdrawn asset
- `amount`/`amount_usd`: Withdrawal quantity including interest
- `block_timestamp`: When withdrawal occurred

## Sample queries

```sql
-- Daily withdrawal patterns
SELECT 
    DATE_TRUNC('day', block_timestamp) AS date,
    platform,
    COUNT(*) AS withdrawal_count,
    COUNT(DISTINCT depositor) AS unique_withdrawers,
    SUM(amount_usd) AS total_withdrawn_usd,
    AVG(amount_usd) AS avg_withdrawal_size_usd
FROM <blockchain_name>.defi.ez_lending_withdraws
WHERE block_timestamp >= CURRENT_DATE - 30
    AND amount_usd IS NOT NULL
GROUP BY 1, 2
ORDER BY 1 DESC, 5 DESC;

-- Platform liquidity analysis (deposits vs withdrawals)
WITH platform_deposits AS (
    SELECT 
        platform,
        token_address,
        token_symbol,
        COUNT(DISTINCT depositor) AS unique_depositors,
        SUM(amount) AS total_deposited_tokens,
        SUM(amount_usd) AS total_deposited_usd,
        COUNT(*) AS deposit_transactions,
        AVG(amount_usd) AS avg_deposit_size_usd
    FROM <blockchain_name>.defi.ez_lending_deposits
    WHERE block_timestamp >= CURRENT_DATE - 90
        AND amount IS NOT NULL
    GROUP BY 1, 2, 3
),
platform_withdrawals AS (
    SELECT 
        platform,
        token_address,
        token_symbol,
        COUNT(DISTINCT depositor) AS unique_withdrawers,
        SUM(amount) AS total_withdrawn_tokens,
        SUM(amount_usd) AS total_withdrawn_usd,
        COUNT(*) AS withdrawal_transactions,
        AVG(amount_usd) AS avg_withdrawal_size_usd
    FROM <blockchain_name>.defi.ez_lending_withdraws
    WHERE block_timestamp >= CURRENT_DATE - 90
        AND amount IS NOT NULL
    GROUP BY 1, 2, 3
)
SELECT 
    COALESCE(d.platform, w.platform) AS platform,
    COALESCE(d.token_address, w.token_address) AS token_address,
    COALESCE(d.token_symbol, w.token_symbol) AS token_symbol,
    d.unique_depositors,
    w.unique_withdrawers,
    d.total_deposited_usd,
    w.total_withdrawn_usd,
    (d.total_deposited_usd - COALESCE(w.total_withdrawn_usd, 0)) AS net_deposits_usd,
    d.deposit_transactions,
    w.withdrawal_transactions,
    d.avg_deposit_size_usd,
    w.avg_withdrawal_size_usd
FROM platform_deposits d
FULL OUTER JOIN platform_withdrawals w
    ON d.platform = w.platform
    AND d.token_address = w.token_address
WHERE COALESCE(d.total_deposited_usd, 0) > 100000
    OR COALESCE(w.total_withdrawn_usd, 0) > 100000
ORDER BY net_deposits_usd DESC;

-- Liquidity stress analysis
WITH hourly_flows AS (
    SELECT 
        DATE_TRUNC('hour', block_timestamp) AS hour,
        platform,
        token_symbol,
        0 AS deposits_usd,
        SUM(amount_usd) AS withdrawals_usd
    FROM <blockchain_name>.defi.ez_lending_withdraws
    WHERE block_timestamp >= CURRENT_DATE - 7
        AND amount_usd IS NOT NULL
    GROUP BY 1, 2, 3
    
    UNION ALL
    
    SELECT 
        DATE_TRUNC('hour', block_timestamp) AS hour,
        platform,
        token_symbol,
        SUM(amount_usd) AS deposits_usd,
        0 AS withdrawals_usd
    FROM <blockchain_name>.defi.ez_lending_deposits
    WHERE block_timestamp >= CURRENT_DATE - 7
        AND amount_usd IS NOT NULL
    GROUP BY 1, 2, 3
)
SELECT 
    hour,
    platform,
    token_symbol,
    SUM(deposits_usd) AS hourly_deposits,
    SUM(withdrawals_usd) AS hourly_withdrawals,
    SUM(deposits_usd - withdrawals_usd) AS net_flow,
    SUM(SUM(deposits_usd - withdrawals_usd)) OVER (
        PARTITION BY platform, token_symbol 
        ORDER BY hour
    ) AS cumulative_flow
FROM hourly_flows
GROUP BY 1, 2, 3
HAVING SUM(withdrawals_usd) > 10000
ORDER BY 1 DESC, 6;

-- Large withdrawals monitoring (potential bank run indicators)
SELECT 
    block_timestamp,
    tx_hash,
    platform,
    depositor,
    token_symbol,
    amount AS withdrawn_tokens,
    amount_usd AS withdrawn_usd,
    LAG(amount_usd) OVER (PARTITION BY platform, token_symbol ORDER BY block_timestamp) AS prev_withdrawal_usd,
    amount_usd / NULLIF(LAG(amount_usd) OVER (PARTITION BY platform, token_symbol ORDER BY block_timestamp), 0) AS size_multiplier
FROM <blockchain_name>.defi.ez_lending_withdraws
WHERE amount_usd > 1000000
    AND block_timestamp >= CURRENT_DATE - 3
ORDER BY withdrawn_usd DESC;

-- Withdrawal timing patterns
SELECT 
    EXTRACT(HOUR FROM block_timestamp) AS hour_of_day,
    COUNT(*) AS withdrawal_count,
    SUM(amount_usd) AS total_withdrawn_usd,
    AVG(amount_usd) AS avg_withdrawal_size
FROM <blockchain_name>.defi.ez_lending_withdraws
WHERE block_timestamp >= CURRENT_DATE - 30
    AND amount_usd IS NOT NULL
GROUP BY 1
ORDER BY 1;
```

{% enddocs %}

{% docs ez_lending_platform %}

The lending protocol where the transaction occurred.

Example: 'aave'

{% enddocs %}

{% docs ez_lending_borrower %}

The address that initiated a borrow or repayment transaction.

Example: '0x1234567890123456789012345678901234567890'

{% enddocs %}

{% docs ez_lending_amount %}

The decimal-adjusted quantity of tokens in the transaction.

Example: 1000.5

{% enddocs %}

{% docs ez_lending_amount_usd %}

The USD value of tokens at transaction time.

Example: 1500.75

{% enddocs %}

{% docs ez_lending_liquidator %}

The address that performed the liquidation.

Example: '0xabcdefabcdefabcdefabcdefabcdefabcdefabcd'

{% enddocs %}

{% docs ez_lending_depositor %}

The address that supplied liquidity to the lending protocol.

Example: '0x9876543210987654321098765432109876543210'

{% enddocs %}

{% docs ez_lending_flashloan_amount_usd %}

The USD value of assets borrowed in a flash loan.

Example: 1000000.50

{% enddocs %}

{% docs ez_lending_protocol_token %}

The lending protocol's receipt token issued to depositors.

Example: '0xfedcbafedcbafedcbafedcbafedcbafedcbafed'

{% enddocs %}

{% docs ez_lending_token_address %}

The contract address of the underlying asset being lent or borrowed.

Example: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'

{% enddocs %}

{% docs ez_lending_token_symbol %}

The ticker symbol of the asset involved in the lending transaction.

Example: 'USDC'

{% enddocs %}

{% docs ez_lending_initiator %}

The address that triggered the flash loan execution.

Example: '0x7a250d5630b4cf539739df2c5dacb4c659f2488d'

{% enddocs %}

{% docs ez_lending_target %}

The contract address that receives and executes the flash loan logic.

Example: '0x1111111254fb6c44bac0bed2854e76f90643097d'

{% enddocs %}

{% docs ez_lending_flashloan_token %}

The contract address of the token borrowed in the flash loan.

Example: '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'

This column will be deprecated October 13th, please update to token_address.

{% enddocs %}

{% docs ez_lending_flashloan_token_symbol %}

The symbol of the token borrowed in the flash loan.

Example: 'WETH'

This column will be deprecated October 13th, please update to token_symbol.

{% enddocs %}

{% docs ez_lending_flashloan_amount_unadj %}

The raw amount of tokens borrowed without decimal adjustment.

Example: 1000000000000000000

{% enddocs %}

{% docs ez_lending_flashloan_amount %}

The decimal-adjusted amount of tokens borrowed in the flash loan.

Example: 1.0

{% enddocs %}

{% docs ez_lending_premium_amount_unadj %}

The raw fee amount charged for the flash loan.

Example: 900000000000000

{% enddocs %}

{% docs ez_lending_premium_amount %}

The decimal-adjusted fee paid for the flash loan.

Example: 0.0009

{% enddocs %}

{% docs ez_lending_premium_amount_usd %}

The USD value of the flash loan fee.

Example: 0.90

{% enddocs %}

{% docs ez_lending_collateral_token %}

The token contract address used as collateral in a liquidation.

Example: '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'

{% enddocs %}

{% docs ez_lending_collateral_token_symbol %}

The symbol of the asset used as collateral in liquidations.

Example: 'WETH'

{% enddocs %}

{% docs ez_lending_debt_token %}

The token contract address that was borrowed and is being repaid in liquidation.

Example: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'

{% enddocs %}

{% docs ez_lending_debt_token_symbol %}

The symbol of the borrowed asset being repaid in liquidation.

Example: 'USDC'

{% enddocs %}

{% docs ez_lending_amount_unadj %}

The raw amount of tokens borrowed or repaid without decimal adjustment.

Example: 1000000000

{% enddocs %}

{% docs ez_lending_payer %}

The address that paid the loan or deposit.

Example: '0x5555555555555555555555555555555555555555'

{% enddocs %}

{% docs ez_lending_ohlc_rates_table_doc %}

## What

This table provides OHLC (Open, High, Low, Close) interest rate data for lending protocols, aggregated by day. It tracks supply, stable borrow, and variable borrow rates with forward-filling for missing data points, enabling analysis of interest rate trends and volatility across lending markets.

## Key Use Cases

- Analyzing interest rate trends and volatility over time
- Comparing rates across different lending protocols and assets
- Monitoring rate changes during market events
- Calculating average rates for yield analysis
- Identifying rate arbitrage opportunities

## Important Relationships

- Links to individual protocol interest rate models (e.g., Aave)
- Can be joined with `ez_lending_deposits` and `ez_lending_borrows` for yield analysis
- References `price.ez_prices_hourly` for USD valuations
- Connects to protocol-specific token contracts

## Commonly-used Fields

- `day`: Date for the OHLC data
- `protocol`/`platform`: Lending protocol details
- `token_address`/`token_symbol`: Asset being tracked
- `supply_rate_*`: Supply interest rate OHLC values
- `stable_borrow_rate_*`: Stable borrow rate OHLC values
- `variable_borrow_rate_*`: Variable borrow rate OHLC values

## Sample queries

```sql
-- Daily interest rate volatility analysis
SELECT 
    day,
    protocol,
    platform,
    token_symbol,
    -- Supply rate volatility
    (supply_rate_high - supply_rate_low) / NULLIF(supply_rate_low, 0) * 100 AS supply_rate_volatility_pct,
    -- Variable borrow rate volatility
    (variable_borrow_rate_high - variable_borrow_rate_low) / NULLIF(variable_borrow_rate_low, 0) * 100 AS variable_borrow_volatility_pct,
    -- Rate spread
    variable_borrow_rate_close - supply_rate_close AS rate_spread,
    rate_updates_count
FROM <blockchain_name>.defi.ez_ohlc_rates
WHERE day >= CURRENT_DATE - 30
    AND token_symbol IS NOT NULL
ORDER BY day DESC, supply_rate_volatility_pct DESC;

-- Protocol comparison - average rates
SELECT 
    protocol,
    platform,
    token_symbol,
    AVG(supply_rate_close) AS avg_supply_rate,
    AVG(variable_borrow_rate_close) AS avg_variable_borrow_rate,
    AVG(stable_borrow_rate_close) AS avg_stable_borrow_rate,
    AVG(variable_borrow_rate_close - supply_rate_close) AS avg_rate_spread,
    COUNT(*) AS days_with_data
FROM <blockchain_name>.defi.ez_ohlc_rates
WHERE day >= CURRENT_DATE - 90
    AND token_symbol IS NOT NULL
GROUP BY 1, 2, 3
HAVING COUNT(*) > 30
ORDER BY avg_rate_spread DESC;

-- Interest rate trends over time
WITH rate_trends AS (
    SELECT 
        day,
        protocol,
        token_symbol,
        supply_rate_close,
        variable_borrow_rate_close,
        stable_borrow_rate_close,
        LAG(supply_rate_close, 7) OVER (PARTITION BY protocol, token_symbol ORDER BY day) AS supply_rate_week_ago,
        LAG(variable_borrow_rate_close, 7) OVER (PARTITION BY protocol, token_symbol ORDER BY day) AS variable_borrow_rate_week_ago
    FROM <blockchain_name>.defi.ez_ohlc_rates
    WHERE day >= CURRENT_DATE - 30
        AND token_symbol IS NOT NULL
)
SELECT 
    day,
    protocol,
    token_symbol,
    supply_rate_close,
    (supply_rate_close - supply_rate_week_ago) / NULLIF(supply_rate_week_ago, 0) * 100 AS supply_rate_change_7d_pct,
    variable_borrow_rate_close,
    (variable_borrow_rate_close - variable_borrow_rate_week_ago) / NULLIF(variable_borrow_rate_week_ago, 0) * 100 AS variable_borrow_rate_change_7d_pct
FROM rate_trends
WHERE supply_rate_week_ago IS NOT NULL
ORDER BY day DESC, ABS(supply_rate_change_7d_pct) DESC;

-- Most volatile interest rate assets
SELECT 
    protocol,
    platform,
    token_symbol,
    STDDEV(supply_rate_close) AS supply_rate_stddev,
    STDDEV(variable_borrow_rate_close) AS variable_borrow_rate_stddev,
    AVG(supply_rate_close) AS avg_supply_rate,
    AVG(variable_borrow_rate_close) AS avg_variable_borrow_rate,
    COUNT(*) AS days_with_data
FROM <blockchain_name>.defi.ez_ohlc_rates
WHERE day >= CURRENT_DATE - 90
    AND token_symbol IS NOT NULL
GROUP BY 1, 2, 3
HAVING COUNT(*) > 30
ORDER BY supply_rate_stddev DESC
LIMIT 20;

-- Rate update frequency analysis
SELECT 
    protocol,
    platform,
    token_symbol,
    AVG(rate_updates_count) AS avg_daily_updates,
    MAX(rate_updates_count) AS max_daily_updates,
    COUNT(*) AS days_with_data,
    SUM(rate_updates_count) AS total_updates
FROM <blockchain_name>.defi.ez_ohlc_rates
WHERE day >= CURRENT_DATE - 30
    AND token_symbol IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY avg_daily_updates DESC;

-- Asset-specific rate analysis
SELECT 
    day,
    protocol,
    platform,
    token_symbol,
    supply_rate_open,
    supply_rate_high,
    supply_rate_low,
    supply_rate_close,
    variable_borrow_rate_open,
    variable_borrow_rate_high,
    variable_borrow_rate_low,
    variable_borrow_rate_close,
    stable_borrow_rate_open,
    stable_borrow_rate_high,
    stable_borrow_rate_low,
    stable_borrow_rate_close,
    rate_updates_count
FROM <blockchain_name>.defi.ez_ohlc_rates
WHERE token_symbol = 'USDC'
    AND protocol = 'aave'
    AND day >= CURRENT_DATE - 7
ORDER BY day DESC;
```

{% enddocs %}

{% docs ez_lending_ohlc_day %}

The date for which the OHLC interest rate data is calculated (truncated to day).

Example: '2024-01-15'

{% enddocs %}

{% docs ez_lending_ohlc_protocol %}

The lending protocol name (e.g., Aave, Compound).

Example: 'aave'

{% enddocs %}

{% docs ez_lending_ohlc_version %}

The version of the protocol (e.g., v2, v3).

Example: 'v3'

{% enddocs %}

{% docs ez_lending_ohlc_supply_rate_open %}

The opening supply interest rate for the day (first rate of the day).

Example: 0.045

{% enddocs %}

{% docs ez_lending_ohlc_supply_rate_high %}

The highest supply interest rate observed during the day.

Example: 0.052

{% enddocs %}

{% docs ez_lending_ohlc_supply_rate_low %}

The lowest supply interest rate observed during the day.

Example: 0.041

{% enddocs %}

{% docs ez_lending_ohlc_supply_rate_close %}

The closing supply interest rate for the day (last rate of the day).

Example: 0.048

{% enddocs %}

{% docs ez_lending_ohlc_stable_borrow_rate_open %}

The opening stable borrow interest rate for the day (first rate of the day).

Example: 0.065

{% enddocs %}

{% docs ez_lending_ohlc_stable_borrow_rate_high %}

The highest stable borrow interest rate observed during the day.

Example: 0.072

{% enddocs %}

{% docs ez_lending_ohlc_stable_borrow_rate_low %}

The lowest stable borrow interest rate observed during the day.

Example: 0.061

{% enddocs %}

{% docs ez_lending_ohlc_stable_borrow_rate_close %}

The closing stable borrow interest rate for the day (last rate of the day).

Example: 0.068

{% enddocs %}

{% docs ez_lending_ohlc_variable_borrow_rate_open %}

The opening variable borrow interest rate for the day (first rate of the day).

Example: 0.085

{% enddocs %}

{% docs ez_lending_ohlc_variable_borrow_rate_high %}

The highest variable borrow interest rate observed during the day.

Example: 0.092

{% enddocs %}

{% docs ez_lending_ohlc_variable_borrow_rate_low %}

The lowest variable borrow interest rate observed during the day.

Example: 0.081

{% enddocs %}

{% docs ez_lending_ohlc_variable_borrow_rate_close %}

The closing variable borrow interest rate for the day (last rate of the day).

Example: 0.088

{% enddocs %}

{% docs ez_lending_ohlc_rate_updates_count %}

The number of rate updates that occurred during the day.

Example: 24

{% enddocs %}

{% docs ez_lending_ohlc_blockchain %}

The blockchain network where the data was sourced from.

Example: 'ethereum'

{% enddocs %}