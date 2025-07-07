{% docs ez_lending_borrows_table_doc %}

## Overview
This table provides a comprehensive view of borrowing transactions across all major lending protocols on EVM blockchains. It captures when users borrow assets against their deposited collateral, enabling analysis of lending market dynamics, user behavior, and protocol performance.

### Key Features
- **Multi-protocol coverage**: Includes Aave, Compound, Morpho, and other major lending protocols
- **Collateralization tracking**: Links borrowing events to underlying collateral positions
- **Rate mode support**: Tracks both stable and variable rate borrowing options
- **USD valuations**: Includes borrowed amounts converted to USD where pricing data is available

### Important Relationships
- Links to `ez_lending_deposits` for collateral analysis
- Joins with `ez_lending_repayments` to track loan lifecycle
- References `ez_lending_liquidations` for risk analysis
- Connects to `price.ez_prices_hourly` for USD valuations

### User Field Mapping
| Table | User Field |
|-------|------------|
| **EZ_LENDING_DEPOSITS** | `depositor` |
| **EZ_LENDING_WITHDRAWS** | `depositor` |
| **EZ_LENDING_BORROWS** | `borrower` |
| **EZ_LENDING_REPAYMENTS** | `borrower` |
| **EZ_LENDING_FLASHLOANS** | `initiator` |
| **EZ_LENDING_LIQUIDATIONS** | `borrower` |

### Join Conditions
**Valid token_address joins:**
- `ez_lending_deposits` ↔ `ez_lending_withdraws` (same asset)
- `ez_lending_borrows` ↔ `ez_lending_repayments` (same borrowed asset)

**Invalid token_address joins:**
- `ez_lending_borrows` ↔ `ez_lending_deposits` (borrowed asset ≠ collateral asset)
- `ez_lending_borrows` ↔ `ez_lending_withdraws` (borrowed asset ≠ collateral asset)

**Note:** When joining borrows to deposits/withdraws, the `token_address` in borrows represents the borrowed asset, while in deposits/withdraws it represents the collateral asset. These are typically different tokens.

### Sample Queries

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

### Critical Usage Notes
- **Collateral requirement**: Users must have deposited collateral before borrowing
- **Trace data**: Some protocols use traces instead of events, causing NULL `event_index` values
- **USD values**: May be NULL for tokens without price data
- **Performance tip**: Always filter by `block_timestamp` and consider indexing on `borrower` for user analysis

### Data Quality Considerations
- Interest rates are stored as raw values and may need scaling (typically by 1e27)
- Some protocols may have incomplete historical data during integration periods
- Cross-protocol comparisons should account for different fee structures
- Flash loan transactions are tracked separately in `defi.ez_lending_flashloans`

{% enddocs %}

{% docs ez_lending_deposits_table_doc %}

## Overview
This table tracks all deposit transactions across lending protocols on EVM blockchains. Deposits represent users supplying liquidity to lending pools, earning yield while enabling their assets to serve as collateral for borrowing.

### Key Features
- **Yield tracking**: Captures supply rates and interest-bearing token issuance
- **Multi-protocol support**: Comprehensive coverage of major lending protocols
- **Collateral enablement**: Deposits can be used as collateral for borrowing
- **Token mapping**: Links underlying assets to protocol-specific receipt tokens

### Important Relationships
- Links to `ez_lending_borrows` for collateralization analysis
- Joins with `ez_lending_withdraws` to track position lifecycle
- References protocol-specific token contracts (aTokens, cTokens, etc.)
- Connects to `price.ez_prices_hourly` for USD valuations

### User Field Mapping
| Table | User Field |
|-------|------------|
| **EZ_LENDING_DEPOSITS** | `depositor` |
| **EZ_LENDING_WITHDRAWS** | `depositor` |
| **EZ_LENDING_BORROWS** | `borrower` |
| **EZ_LENDING_REPAYMENTS** | `borrower` |
| **EZ_LENDING_FLASHLOANS** | `initiator` |
| **EZ_LENDING_LIQUIDATIONS** | `borrower` |

### Join Conditions
**Valid token_address joins:**
- `ez_lending_deposits` ↔ `ez_lending_withdraws` (same asset)
- `ez_lending_borrows` ↔ `ez_lending_repayments` (same borrowed asset)

**Invalid token_address joins:**
- `ez_lending_borrows` ↔ `ez_lending_deposits` (borrowed asset ≠ collateral asset)
- `ez_lending_borrows` ↔ `ez_lending_withdraws` (borrowed asset ≠ collateral asset)

**Note:** When joining borrows to deposits/withdraws, the `token_address` in borrows represents the borrowed asset, while in deposits/withdraws it represents the collateral asset. These are typically different tokens.

### Sample Queries

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

### Critical Usage Notes
- **Receipt tokens**: Protocols issue interest-bearing tokens (aTokens, cTokens) representing deposits
- **Supply rates**: Rates are dynamic and update with market conditions
- **Collateral status**: Not all deposits are automatically enabled as collateral
- **Trace data**: Some protocols use traces, resulting in NULL `event_index` values
- **Performance tip**: Index on `depositor` and `block_timestamp` for efficient queries

### Data Quality Considerations
- Supply rates may need scaling depending on protocol (check documentation)
- USD values depend on price feed availability
- Some protocols have migration events that may show as large deposits/withdrawals
- Interest accrual happens continuously but is realized on withdrawal

{% enddocs %}

{% docs ez_lending_flashloans_table_doc %}

## Overview
This table captures flash loan transactions across lending protocols. Flash loans enable borrowing without collateral within a single transaction, provided the loan plus fees are repaid before transaction completion. This advanced DeFi primitive is primarily used for arbitrage, collateral swapping, and liquidations.

### Key Features
- **Zero-collateral loans**: Borrow any available liquidity without collateral
- **Atomic transactions**: Loan must be repaid within the same transaction
- **Fee tracking**: Captures premium amounts charged by protocols
- **Use case insights**: Enables analysis of DeFi composability and efficiency

### Important Relationships
- Often precedes transactions in DEX tables for arbitrage analysis
- Links to `ez_lending_liquidations` for liquidation strategies
- May connect to multiple protocols within single transaction
- References `price.ez_prices_hourly` for USD valuations

### User Field Mapping
| Table | User Field |
|-------|------------|
| **EZ_LENDING_DEPOSITS** | `depositor` |
| **EZ_LENDING_WITHDRAWS** | `depositor` |
| **EZ_LENDING_BORROWS** | `borrower` |
| **EZ_LENDING_REPAYMENTS** | `borrower` |
| **EZ_LENDING_FLASHLOANS** | `initiator` |
| **EZ_LENDING_LIQUIDATIONS** | `borrower` |

### Join Conditions
**Valid token_address joins:**
- `ez_lending_deposits` ↔ `ez_lending_withdraws` (same asset)
- `ez_lending_borrows` ↔ `ez_lending_repayments` (same borrowed asset)

**Invalid token_address joins:**
- `ez_lending_borrows` ↔ `ez_lending_deposits` (borrowed asset ≠ collateral asset)
- `ez_lending_borrows` ↔ `ez_lending_withdraws` (borrowed asset ≠ collateral asset)

**Note:** When joining borrows to deposits/withdraws, the `token_address` in borrows represents the borrowed asset, while in deposits/withdraws it represents the collateral asset. These are typically different tokens.

### Sample Queries

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

### Critical Usage Notes
- **Developer feature**: Requires smart contract interaction, not available via UI
- **Atomic requirement**: Transaction reverts if loan not repaid
- **Gas intensive**: Flash loans typically have high gas costs
- **Premium amounts**: Fees vary by protocol (typically 0.05% - 0.09%)
- **Performance tip**: Flash loans often appear in complex transactions with multiple events

### Data Quality Considerations
- Target address may be intermediary contract, not final beneficiary
- Some protocols emit multiple events for single flash loan
- Large flash loans often indicate arbitrage or liquidation activity
- Premium calculations should account for token decimals

{% enddocs %}

{% docs ez_lending_liquidations_table_doc %}

## Overview
This table tracks liquidation events across lending protocols, where under-collateralized positions are forcibly closed to protect protocol solvency. Liquidations occur when a borrower's health factor drops below 1, typically due to collateral value decline or debt value increase.

### Key Features
- **Risk management**: Critical for understanding protocol and user risk
- **Liquidator rewards**: Tracks liquidation incentives (typically 5-10% bonus)
- **Health factor tracking**: Monitors collateralization ratios
- **Multi-asset support**: Handles liquidations across different collateral/debt pairs

### Important Relationships
- Links to `ez_lending_borrows` for original loan details
- Connects to `ez_lending_deposits` for collateral information
- Often preceded by entries in `ez_lending_flashloans`
- References `price.ez_prices_hourly` for USD valuations

### User Field Mapping
| Table | User Field |
|-------|------------|
| **EZ_LENDING_DEPOSITS** | `depositor` |
| **EZ_LENDING_WITHDRAWS** | `depositor` |
| **EZ_LENDING_BORROWS** | `borrower` |
| **EZ_LENDING_REPAYMENTS** | `borrower` |
| **EZ_LENDING_FLASHLOANS** | `initiator` |
| **EZ_LENDING_LIQUIDATIONS** | `borrower` |

### Join Conditions
**Valid token_address joins:**
- `ez_lending_deposits` ↔ `ez_lending_withdraws` (same asset)
- `ez_lending_borrows` ↔ `ez_lending_repayments` (same borrowed asset)

**Invalid token_address joins:**
- `ez_lending_borrows` ↔ `ez_lending_deposits` (borrowed asset ≠ collateral asset)
- `ez_lending_borrows` ↔ `ez_lending_withdraws` (borrowed asset ≠ collateral asset)

**Note:** When joining borrows to deposits/withdraws, the `token_address` in borrows represents the borrowed asset, while in deposits/withdraws it represents the collateral asset. These are typically different tokens.

### Sample Queries

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

### Critical Usage Notes
- **Partial liquidations**: Up to 50% of debt can be liquidated per transaction
- **Liquidation bonus**: Liquidators receive collateral worth more than debt repaid
- **MEV activity**: Many liquidations are performed by MEV bots
- **Health factor**: < 1 triggers liquidation eligibility
- **Performance tip**: Large liquidations often correlate with market volatility

### Data Quality Considerations
- Some protocols use different liquidation mechanisms (auctions vs fixed discount)
- Liquidation bonuses vary by protocol and asset risk parameters
- Flash loans are commonly used to perform liquidations without capital
- High-frequency liquidators may use multiple addresses

{% enddocs %}

{% docs ez_lending_repayments_table_doc %}

## Overview
This table contains loan repayment transactions across lending protocols. Repayments reduce or eliminate outstanding debt positions, with amounts including both principal and accrued interest. Understanding repayment patterns helps analyze user behavior and protocol health.

### Key Features
- **Interest tracking**: Repayments include accrued interest since borrowing
- **Partial/full support**: Tracks both partial and complete loan repayments
- **Multi-asset coverage**: Handles repayments in various borrowed assets
- **Rate mode aware**: Distinguishes between stable and variable rate repayments

### Important Relationships
- Links to `ez_lending_borrows` for original loan details
- Connects to `ez_lending_liquidations` (forced repayments)
- May reference `ez_lending_deposits` for collateral release
- Uses `price.ez_prices_hourly` for USD valuations

### User Field Mapping
| Table | User Field |
|-------|------------|
| **EZ_LENDING_DEPOSITS** | `depositor` |
| **EZ_LENDING_WITHDRAWS** | `depositor` |
| **EZ_LENDING_BORROWS** | `borrower` |
| **EZ_LENDING_REPAYMENTS** | `borrower` |
| **EZ_LENDING_FLASHLOANS** | `initiator` |
| **EZ_LENDING_LIQUIDATIONS** | `borrower` |

### Join Conditions
**Valid token_address joins:**
- `ez_lending_deposits` ↔ `ez_lending_withdraws` (same asset)
- `ez_lending_borrows` ↔ `ez_lending_repayments` (same borrowed asset)

**Invalid token_address joins:**
- `ez_lending_borrows` ↔ `ez_lending_deposits` (borrowed asset ≠ collateral asset)
- `ez_lending_borrows` ↔ `ez_lending_withdraws` (borrowed asset ≠ collateral asset)

**Note:** When joining borrows to deposits/withdraws, the `token_address` in borrows represents the borrowed asset, while in deposits/withdraws it represents the collateral asset. These are typically different tokens.

### Sample Queries

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

### Critical Usage Notes
- **Interest inclusion**: Repaid amounts include both principal and accrued interest
- **Payer vs borrower**: `payer` may differ from `borrower` for third-party repayments
- **Over-repayment**: Some users repay more than owed, creating a deposit
- **Trace data**: Some protocols use traces, resulting in NULL `event_index`
- **Performance tip**: Join with borrows carefully due to multiple repayments per loan

### Data Quality Considerations
- Interest calculations depend on time elapsed and rate changes
- Some protocols allow repayment in different assets (converted at repayment)
- Partial repayments are common for large loans
- Liquidations may appear as forced repayments in some protocols

{% enddocs %}

{% docs ez_lending_withdraws_table_doc %}

## Overview
This table tracks withdrawal transactions where users remove their supplied liquidity from lending protocols. Withdrawals include the original deposit plus earned interest, subject to available liquidity in the protocol.

### Key Features
- **Interest inclusion**: Withdrawn amounts include earned yield
- **Liquidity dependent**: Withdrawals require sufficient unborrowed liquidity
- **Receipt token burning**: Protocol tokens (aTokens, cTokens) are burned
- **Position tracking**: Enables calculation of user positions over time

### Important Relationships
- Links to `ez_lending_deposits` for position lifecycle
- Affected by `ez_lending_borrows` (reduces available liquidity)
- Increased by `ez_lending_repayments` (adds liquidity)
- References `price.ez_prices_hourly` for USD valuations

### User Field Mapping
| Table | User Field |
|-------|------------|
| **EZ_LENDING_DEPOSITS** | `depositor` |
| **EZ_LENDING_WITHDRAWS** | `depositor` |
| **EZ_LENDING_BORROWS** | `borrower` |
| **EZ_LENDING_REPAYMENTS** | `borrower` |
| **EZ_LENDING_FLASHLOANS** | `initiator` |
| **EZ_LENDING_LIQUIDATIONS** | `borrower` |

### Join Conditions
**Valid token_address joins:**
- `ez_lending_deposits` ↔ `ez_lending_withdraws` (same asset)
- `ez_lending_borrows` ↔ `ez_lending_repayments` (same borrowed asset)

**Invalid token_address joins:**
- `ez_lending_borrows` ↔ `ez_lending_deposits` (borrowed asset ≠ collateral asset)
- `ez_lending_borrows` ↔ `ez_lending_withdraws` (borrowed asset ≠ collateral asset)

**Note:** When joining borrows to deposits/withdraws, the `token_address` in borrows represents the borrowed asset, while in deposits/withdraws it represents the collateral asset. These are typically different tokens.

### Sample Queries

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

### Critical Usage Notes
- **Liquidity requirement**: Withdrawals fail if insufficient unborrowed liquidity
- **Interest earned**: Withdrawal amounts exceed deposits due to earned interest
- **Protocol tokens**: Interest-bearing tokens are burned on withdrawal
- **Trace data**: Some protocols use traces, causing NULL `event_index`
- **Performance tip**: Large withdrawals may indicate protocol stress

### Data Quality Considerations
- Withdrawn amounts include accumulated interest since deposit
- Some protocols have withdrawal limits or timelock mechanisms
- Emergency withdrawals may bypass normal processes
- Protocol migrations may show as large withdrawal events

{% enddocs %}

{% docs ez_lending_platform %}

## Platform
The lending protocol where the transaction occurred.

### Details
- **Type**: `VARCHAR`
- **Common values**: `'aave'`, `'compound'`, `'maker'`, `'venus'`, `'benqi'`, `'moonwell'`
- **Case**: Lowercase standardized protocol names

### Usage Examples
```sql
-- Protocol comparison
SELECT 
    platform,
    COUNT(DISTINCT borrower) AS unique_users,
    SUM(amount_usd) AS total_volume_usd
FROM <blockchain_name>.defi.ez_lending_borrows
WHERE block_timestamp >= CURRENT_DATE - 30
GROUP BY platform
ORDER BY total_volume_usd DESC;

-- Cross-protocol user analysis
SELECT 
    borrower,
    ARRAY_AGG(DISTINCT platform) AS protocols_used,
    COUNT(DISTINCT platform) AS protocol_count
FROM <blockchain_name>.defi.ez_lending_borrows
GROUP BY borrower
HAVING COUNT(DISTINCT platform) > 1;
```

### Notes
- Platform names are standardized across all lending tables
- New protocols added as they gain traction
- Some platforms have multiple versions (e.g., 'aave_v2', 'aave_v3')

{% enddocs %}

{% docs ez_lending_borrower %}

## Borrower
The address that initiated a borrow or repayment transaction.

### Details
- **Type**: `VARCHAR(42)`
- **Format**: `0x` prefixed Ethereum address
- **Usage**: Links borrowing activity across tables

### Usage Examples
```sql
-- Borrower health analysis
WITH borrower_stats AS (
    SELECT 
        b.borrower,
        SUM(b.amount_usd) AS total_borrowed,
        SUM(r.amount_usd) AS total_repaid,
        COUNT(DISTINCT b.platform) AS platforms_used
    FROM <blockchain_name>.defi.ez_lending_borrows b
    LEFT JOIN <blockchain_name>.defi.ez_lending_repayments r
        ON b.borrower = r.borrower
    WHERE b.block_timestamp >= CURRENT_DATE - 90
    GROUP BY b.borrower
)
SELECT 
    borrower,
    total_borrowed,
    total_repaid,
    total_borrowed - COALESCE(total_repaid, 0) AS outstanding_estimate,
    platforms_used
FROM borrower_stats
WHERE total_borrowed > 10000
ORDER BY outstanding_estimate DESC;

-- Borrower liquidation risk
SELECT 
    b.borrower,
    COUNT(DISTINCT l.tx_hash) AS times_liquidated,
    SUM(b.amount_usd) AS total_borrowed,
    SUM(l.amount_usd) AS total_liquidated
FROM <blockchain_name>.defi.ez_lending_borrows b
LEFT JOIN <blockchain_name>.defi.ez_lending_liquidations l
    ON b.borrower = l.borrower
GROUP BY b.borrower
HAVING COUNT(DISTINCT l.tx_hash) > 0;
```

{% enddocs %}

{% docs ez_lending_amount %}

## Amount
The decimal-adjusted quantity of tokens in the transaction.

### Details
- **Type**: `NUMERIC`
- **Calculation**: `amount_unadj / 10^decimals`
- **NULL cases**: When token decimal information unavailable

### Usage Examples
```sql
-- Large transactions by amount
SELECT 
    platform,
    token_symbol,
    amount,
    amount_usd,
    tx_hash
FROM <blockchain_name>.defi.ez_lending_borrows
WHERE amount IS NOT NULL
    AND token_symbol = 'WETH'
ORDER BY amount DESC
LIMIT 100;

-- Average transaction sizes
SELECT 
    token_symbol,
    AVG(amount) AS avg_amount,
    STDDEV(amount) AS stddev_amount,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) AS median_amount
FROM <blockchain_name>.defi.ez_lending_deposits
WHERE amount IS NOT NULL
GROUP BY token_symbol
HAVING COUNT(*) > 1000;
```

{% enddocs %}

{% docs ez_lending_amount_usd %}

## Amount USD
The USD value of tokens at transaction time.

### Details
- **Type**: `NUMERIC`
- **Source**: Calculated using price feeds at block timestamp
- **NULL cases**: No price data available for token

### Usage Examples
```sql
-- Daily USD volume across all actions
SELECT 
    DATE_TRUNC('day', block_timestamp) AS date,
    'deposits' AS action,
    SUM(amount_usd) AS volume_usd
FROM <blockchain_name>.defi.ez_lending_deposits
WHERE block_timestamp >= CURRENT_DATE - 30
    AND amount_usd IS NOT NULL
GROUP BY date

UNION ALL

SELECT 
    DATE_TRUNC('day', block_timestamp) AS date,
    'borrows' AS action,
    SUM(amount_usd) AS volume_usd
FROM <blockchain_name>.defi.ez_lending_borrows
WHERE block_timestamp >= CURRENT_DATE - 30
    AND amount_usd IS NOT NULL
GROUP BY date

ORDER BY date DESC, action;

-- Price coverage analysis
SELECT 
    platform,
    COUNT(*) AS total_transactions,
    COUNT(amount_usd) AS priced_transactions,
    COUNT(amount_usd) * 100.0 / COUNT(*) AS coverage_pct
FROM <blockchain_name>.defi.ez_lending_deposits
GROUP BY platform
ORDER BY coverage_pct DESC;
```

### Performance Notes
- Always filter `amount_usd IS NOT NULL` for financial calculations
- USD values enable cross-asset comparisons
- Price data may have slight delays during high volatility

{% enddocs %}

{% docs ez_lending_liquidator %}

## Liquidator
The address that performed the liquidation.

### Details
- **Type**: `VARCHAR(42)`
- **Pattern**: Often automated bots or MEV searchers
- **Incentive**: Receives liquidation bonus (typically 5-10%)

### Usage Examples
```sql
-- Top liquidators by profit
SELECT 
    liquidator,
    COUNT(*) AS liquidations_performed,
    SUM(amount_usd - amount_usd) AS total_profit_usd,
    AVG(amount_usd - amount_usd) AS avg_profit_per_liquidation,
    COUNT(DISTINCT platform) AS platforms_used
FROM <blockchain_name>.defi.ez_lending_liquidations
WHERE amount_usd IS NOT NULL
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY liquidator
ORDER BY total_profit_usd DESC
LIMIT 50;

-- Liquidator competition analysis
WITH liquidation_timing AS (
    SELECT 
        liquidator,
        block_timestamp,
        LEAD(block_timestamp) OVER (PARTITION BY borrower ORDER BY block_timestamp) AS next_liquidation_time
    FROM <blockchain_name>.defi.ez_lending_liquidations
)
SELECT 
    liquidator,
    COUNT(*) AS first_liquidations,
    AVG(EXTRACT(EPOCH FROM (next_liquidation_time - block_timestamp))) AS avg_seconds_before_next
FROM liquidation_timing
WHERE next_liquidation_time IS NOT NULL
    AND next_liquidation_time - block_timestamp < INTERVAL '1 minute'
GROUP BY liquidator
HAVING COUNT(*) > 10
ORDER BY avg_seconds_before_next;
```

### Notes
- Professional liquidators often use flash loans
- MEV bots dominate liquidation activity
- Some liquidators specialize in specific protocols or assets

{% enddocs %}

{% docs ez_lending_depositor %}

## Depositor
The address that supplied liquidity to the lending protocol.

### Details
- **Type**: `VARCHAR(42)`
- **Purpose**: Tracks liquidity providers
- **Relationship**: May also appear as borrower using deposits as collateral

### Usage Examples
```sql
-- Depositor loyalty analysis
SELECT 
    depositor,
    MIN(block_timestamp) AS first_deposit,
    MAX(block_timestamp) AS last_deposit,
    COUNT(DISTINCT DATE_TRUNC('month', block_timestamp)) AS active_months,
    COUNT(DISTINCT platform) AS platforms_used,
    SUM(amount_usd) AS total_deposited_usd
FROM <blockchain_name>.defi.ez_lending_deposits
WHERE amount_usd IS NOT NULL
GROUP BY depositor
HAVING COUNT(*) > 10
ORDER BY total_deposited_usd DESC;

-- Depositor concentration
WITH platform_deposits AS (
    SELECT 
        platform,
        depositor,
        SUM(amount_usd) AS depositor_total_usd
    FROM <blockchain_name>.defi.ez_lending_deposits
    WHERE block_timestamp >= CURRENT_DATE - 30
        AND amount_usd IS NOT NULL
    GROUP BY platform, depositor
),
platform_totals AS (
    SELECT 
        platform,
        SUM(depositor_total_usd) AS platform_total_usd
    FROM platform_deposits
    GROUP BY platform
)
SELECT 
    pd.platform,
    COUNT(DISTINCT pd.depositor) AS total_depositors,
    SUM(CASE WHEN pd.depositor_total_usd > pt.platform_total_usd * 0.01 THEN 1 ELSE 0 END) AS whale_depositors,
    SUM(CASE WHEN pd.depositor_total_usd > pt.platform_total_usd * 0.01 THEN pd.depositor_total_usd ELSE 0 END) / pt.platform_total_usd * 100 AS whale_concentration_pct
FROM platform_deposits pd
JOIN platform_totals pt ON pd.platform = pt.platform
GROUP BY pd.platform, pt.platform_total_usd
ORDER BY whale_concentration_pct DESC;
```

{% enddocs %}

{% docs ez_lending_flashloan_amount_usd %}

## Flash Loan Amount USD
The USD value of assets borrowed in a flash loan.

### Details
- **Type**: `NUMERIC`
- **Characteristics**: Often very large amounts (millions)
- **Use cases**: Arbitrage, liquidations, collateral swapping

### Usage Examples
```sql
-- Flash loan size distribution
SELECT 
    CASE 
        WHEN flashloan_amount_usd < 10000 THEN '< $10K'
        WHEN flashloan_amount_usd < 100000 THEN '$10K - $100K'
        WHEN flashloan_amount_usd < 1000000 THEN '$100K - $1M'
        WHEN flashloan_amount_usd < 10000000 THEN '$1M - $10M'
        ELSE '> $10M'
    END AS size_bucket,
    COUNT(*) AS loan_count,
    SUM(flashloan_amount_usd) AS total_volume,
    SUM(premium_amount_usd) AS total_fees
FROM <blockchain_name>.defi.ez_lending_flashloans
WHERE flashloan_amount_usd IS NOT NULL
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY size_bucket
ORDER BY MIN(flashloan_amount_usd);

-- Flash loan profitability estimation
SELECT 
    initiator,
    COUNT(*) AS flashloan_count,
    SUM(flashloan_amount_usd) AS total_borrowed_usd,
    SUM(premium_amount_usd) AS total_fees_paid,
    AVG(premium_amount_usd / NULLIF(flashloan_amount_usd, 0) * 100) AS avg_fee_rate_pct
FROM <blockchain_name>.defi.ez_lending_flashloans
WHERE flashloan_amount_usd > 100000
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY initiator
HAVING COUNT(*) > 5
ORDER BY total_borrowed_usd DESC;
```

{% enddocs %}

{% docs ez_lending_protocol_token %}

## Protocol Token
The lending protocol's receipt token issued to depositors.

### Details
- **Type**: `VARCHAR(42)`
- **Purpose**: Interest-bearing token representing deposited position
- **Examples**: aTokens (Aave), cTokens (Compound), vTokens (Venus)
- **Mechanism**: Minted on deposit, burned on withdrawal

### Usage Examples
```sql
-- Protocol token analysis
SELECT 
    platform,
    token_symbol AS underlying_asset,
    protocol_token,
    COUNT(*) AS deposit_count,
    SUM(issued_tokens) AS total_tokens_issued
FROM <blockchain_name>.defi.ez_lending_deposits
WHERE protocol_token IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 5 DESC;

-- Track protocol token transfers (position transfers)
SELECT 
    d.platform,
    d.protocol_token,
    d.token_symbol AS underlying,
    COUNT(DISTINCT d.depositor) AS unique_depositors,
    SUM(d.issued_tokens) AS total_issued
FROM <blockchain_name>.defi.ez_lending_deposits d
WHERE d.protocol_token IS NOT NULL
    AND d.block_timestamp >= CURRENT_DATE - 30
GROUP BY 1, 2, 3
ORDER BY 5 DESC;
```

### Notes
- Protocol tokens automatically accrue interest
- Can be transferred between addresses
- Exchange rate to underlying increases over time
- Some protocols use rebasing tokens instead

{% enddocs %}

{% docs ez_lending_token_address %}

## Token Address
The contract address of the underlying asset being lent or borrowed.

### Details
- **Type**: `VARCHAR(42)`
- **Format**: `0x` prefixed Ethereum address
- **NULL cases**: NULL for native assets (ETH)
- **Standards**: Typically ERC-20 tokens

### Usage Examples
```sql
-- Most active token addresses
SELECT 
    token_address,
    token_symbol,
    COUNT(DISTINCT platform) AS platforms_supporting,
    COUNT(*) AS total_transactions,
    SUM(amount_usd) AS total_volume_usd
FROM <blockchain_name>.defi.ez_lending_borrows
WHERE token_address IS NOT NULL
    AND amount_usd IS NOT NULL
GROUP BY 1, 2
ORDER BY 5 DESC
LIMIT 50;

-- Cross-platform token analysis
WITH token_platforms AS (
    SELECT 
        token_address,
        platform,
        AVG(borrow_rate_variable) AS avg_borrow_rate,
        COUNT(*) AS transaction_count
    FROM <blockchain_name>.defi.ez_lending_borrows
    WHERE token_address IS NOT NULL
        AND borrow_rate_variable IS NOT NULL
        AND block_timestamp >= CURRENT_DATE - 7
    GROUP BY 1, 2
)
SELECT 
    token_address,
    COUNT(DISTINCT platform) AS platform_count,
    MIN(avg_borrow_rate) AS min_rate,
    MAX(avg_borrow_rate) AS max_rate,
    MAX(avg_borrow_rate) - MIN(avg_borrow_rate) AS rate_spread
FROM token_platforms
GROUP BY 1
HAVING COUNT(DISTINCT platform) > 1
ORDER BY rate_spread DESC;
```

### Performance Notes
- Index on token_address for efficient filtering
- Join with token metadata tables for additional info
- Consider lowercase normalization for consistency

{% enddocs %}

{% docs ez_lending_token_symbol %}

## Token Symbol
The ticker symbol of the asset involved in the lending transaction.

### Details
- **Type**: `VARCHAR`
- **Common values**: `'USDC'`, `'USDT'`, `'WETH'`, `'DAI'`, `'WBTC'`
- **Standards**: Uppercase convention
- **NULL handling**: May be NULL for unverified tokens

### Usage Examples
```sql
-- Stablecoin lending dominance
SELECT 
    token_symbol,
    COUNT(DISTINCT borrower) AS unique_borrowers,
    SUM(amount_usd) AS total_borrowed_usd,
    AVG(borrow_rate_variable) AS avg_variable_rate,
    CASE 
        WHEN token_symbol IN ('USDC', 'USDT', 'DAI', 'BUSD', 'FRAX') THEN 'Stablecoin'
        WHEN token_symbol IN ('WETH', 'WBTC') THEN 'Major Crypto'
        ELSE 'Other'
    END AS asset_category
FROM <blockchain_name>.defi.ez_lending_borrows
WHERE token_symbol IS NOT NULL
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY token_symbol
ORDER BY total_borrowed_usd DESC;

-- Symbol consistency check across platforms
SELECT 
    token_address,
    COUNT(DISTINCT token_symbol) AS symbol_variations,
    ARRAY_AGG(DISTINCT token_symbol) AS symbols_used,
    COUNT(DISTINCT platform) AS platforms
FROM <blockchain_name>.defi.ez_lending_deposits
WHERE token_address IS NOT NULL
GROUP BY token_address
HAVING COUNT(DISTINCT token_symbol) > 1;
```

{% enddocs %}

{% docs ez_lending_initiator_address %}

## Initiator Address
The address that triggered the flash loan execution.

### Details
- **Type**: `VARCHAR(42)`
- **Pattern**: Usually a smart contract, rarely an EOA
- **Purpose**: Identifies the flash loan originator
- **Relationship**: May differ from `target_address` in complex transactions

### Usage Examples
```sql
-- Flash loan initiator patterns
SELECT 
    initiator,
    COUNT(*) AS flashloans_initiated,
    COUNT(DISTINCT platform) AS platforms_used,
    COUNT(DISTINCT flashloan_token) AS unique_tokens_borrowed,
    SUM(flashloan_amount_usd) AS total_volume_usd,
    AVG(premium_amount_usd / NULLIF(flashloan_amount_usd, 0) * 100) AS avg_fee_rate_pct
FROM <blockchain_name>.defi.ez_lending_flashloans
WHERE flashloan_amount_usd IS NOT NULL
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY initiator
ORDER BY total_volume_usd DESC
LIMIT 100;

-- Initiator specialization analysis
WITH initiator_tokens AS (
    SELECT 
        initiator,
        flashloan_token_symbol,
        COUNT(*) AS token_flashloans,
        SUM(flashloan_amount_usd) AS token_volume
    FROM <blockchain_name>.defi.ez_lending_flashloans
    WHERE flashloan_token_symbol IS NOT NULL
    GROUP BY 1, 2
),
initiator_totals AS (
    SELECT 
        initiator,
        SUM(token_volume) AS total_volume
    FROM initiator_tokens
    GROUP BY 1
)
SELECT 
    it.initiator,
    it.flashloan_token_symbol,
    it.token_volume / t.total_volume * 100 AS token_concentration_pct,
    it.token_flashloans
FROM initiator_tokens it
JOIN initiator_totals t ON it.initiator = t.initiator
WHERE it.token_volume / t.total_volume > 0.5
ORDER BY token_concentration_pct DESC;
```

### Notes
- Professional arbitrageurs often use dedicated contracts
- Same initiator may use multiple target addresses
- High-frequency initiators likely automated bots

{% enddocs %}

{% docs ez_lending_target_address %}

## Target Address
The contract address that receives and executes the flash loan logic.

### Details
- **Type**: `VARCHAR(42)`
- **Purpose**: Executes arbitrage, liquidation, or other DeFi operations
- **Pattern**: Smart contract implementing flash loan callback interface
- **Security**: Must repay loan + fee or transaction reverts

### Usage Examples
```sql
-- Target address activity analysis
SELECT 
    target_address,
    initiator,
    COUNT(*) AS flashloan_count,
    COUNT(DISTINCT platform) AS platforms_used,
    SUM(flashloan_amount_usd) AS total_executed_usd,
    MAX(flashloan_amount_usd) AS largest_loan_usd
FROM <blockchain_name>.defi.ez_lending_flashloans
WHERE block_timestamp >= CURRENT_DATE - 30
    AND flashloan_amount_usd IS NOT NULL
GROUP BY 1, 2
ORDER BY total_executed_usd DESC
LIMIT 50;

-- Target contract reuse patterns
SELECT 
    target_address,
    COUNT(DISTINCT initiator) AS unique_initiators,
    COUNT(DISTINCT DATE_TRUNC('day', block_timestamp)) AS active_days,
    SUM(flashloan_amount_usd) AS total_volume,
    ARRAY_AGG(DISTINCT platform) AS platforms_used
FROM <blockchain_name>.defi.ez_lending_flashloans
WHERE block_timestamp >= CURRENT_DATE - 90
GROUP BY target_address
HAVING COUNT(DISTINCT initiator) > 1
ORDER BY unique_initiators DESC;
```

{% enddocs %}

{% docs ez_lending_flashloan_token %}

## Flash Loan Token
The contract address of the token borrowed in the flash loan.

### Details
- **Type**: `VARCHAR(42)`
- **Purpose**: Identifies specific asset borrowed
- **Common patterns**: Stablecoins for arbitrage, WETH for liquidations
- **Liquidity**: Limited by available protocol reserves

### Usage Examples
```sql
-- Flash loan token preferences
SELECT 
    flashloan_token,
    flashloan_token_symbol,
    COUNT(*) AS loan_count,
    SUM(flashloan_amount) AS total_borrowed,
    AVG(flashloan_amount_usd) AS avg_loan_size_usd,
    MAX(flashloan_amount_usd) AS max_loan_usd
FROM <blockchain_name>.defi.ez_lending_flashloans
WHERE flashloan_token IS NOT NULL
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY 1, 2
ORDER BY loan_count DESC;

-- Multi-token flash loans (same transaction)
WITH tx_flashloans AS (
    SELECT 
        tx_hash,
        COUNT(DISTINCT flashloan_token) AS token_count,
        ARRAY_AGG(DISTINCT flashloan_token_symbol) AS tokens_borrowed,
        SUM(flashloan_amount_usd) AS total_tx_usd
    FROM <blockchain_name>.defi.ez_lending_flashloans
    WHERE flashloan_amount_usd IS NOT NULL
    GROUP BY tx_hash
)
SELECT 
    token_count,
    COUNT(*) AS transaction_count,
    AVG(total_tx_usd) AS avg_total_borrowed_usd,
    MODE() WITHIN GROUP (ORDER BY tokens_borrowed) AS common_token_combo
FROM tx_flashloans
WHERE token_count > 1
GROUP BY token_count
ORDER BY token_count;
```

{% enddocs %}

{% docs ez_lending_flashloan_token_symbol %}

## Flash Loan Token Symbol
The symbol of the token borrowed in the flash loan.

### Details
- **Type**: `VARCHAR`
- **Common values**: `'USDC'`, `'DAI'`, `'WETH'`, `'USDT'`, `'WBTC'`
- **Purpose**: Human-readable token identifier
- **Case**: Standardized to uppercase

### Usage Examples
```sql
-- Flash loan token popularity by use case
WITH flashloan_patterns AS (
    SELECT 
        flashloan_token_symbol,
        CASE 
            WHEN flashloan_amount_usd < 100000 THEN 'Small (<$100K)'
            WHEN flashloan_amount_usd < 1000000 THEN 'Medium ($100K-$1M)'
            WHEN flashloan_amount_usd < 10000000 THEN 'Large ($1M-$10M)'
            ELSE 'Whale (>$10M)'
        END AS size_category,
        COUNT(*) AS loan_count,
        SUM(premium_amount_usd) AS total_fees_usd
    FROM <blockchain_name>.defi.ez_lending_flashloans
    WHERE flashloan_token_symbol IS NOT NULL
        AND flashloan_amount_usd IS NOT NULL
        AND block_timestamp >= CURRENT_DATE - 30
    GROUP BY 1, 2
)
SELECT 
    flashloan_token_symbol,
    size_category,
    loan_count,
    total_fees_usd,
    loan_count * 100.0 / SUM(loan_count) OVER (PARTITION BY flashloan_token_symbol) AS pct_of_token_loans
FROM flashloan_patterns
ORDER BY flashloan_token_symbol, loan_count DESC;
```

{% enddocs %}

{% docs ez_lending_flashloan_amount_unadj %}

## Flash Loan Amount Unadjusted
The raw amount of tokens borrowed without decimal adjustment.

### Details
- **Type**: `NUMERIC`
- **Usage**: Raw blockchain value
- **Relationship**: `flashloan_amount = flashloan_amount_unadj / 10^decimals`

### Usage Examples
```sql
-- Decimal verification for flash loan tokens
SELECT 
    flashloan_token_symbol,
    flashloan_token,
    AVG(LOG(10, flashloan_amount_unadj::FLOAT / NULLIF(flashloan_amount::FLOAT, 0))) AS implied_decimals,
    COUNT(*) AS sample_size
FROM <blockchain_name>.defi.ez_lending_flashloans
WHERE flashloan_amount > 0
    AND flashloan_amount_unadj > 0
    AND flashloan_token_symbol IS NOT NULL
GROUP BY 1, 2
HAVING COUNT(*) > 10
ORDER BY flashloan_token_symbol;
```

{% enddocs %}

{% docs ez_lending_flashloan_amount %}

## Flash Loan Amount
The decimal-adjusted amount of tokens borrowed in the flash loan.

### Details
- **Type**: `NUMERIC`
- **Calculation**: Adjusted for token decimals
- **Scale**: Can be very large (millions of tokens)

### Usage Examples
```sql
-- Flash loan size distribution by token
SELECT 
    flashloan_token_symbol,
    MIN(flashloan_amount) AS min_amount,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY flashloan_amount) AS p25,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY flashloan_amount) AS median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY flashloan_amount) AS p75,
    MAX(flashloan_amount) AS max_amount,
    AVG(flashloan_amount) AS avg_amount
FROM <blockchain_name>.defi.ez_lending_flashloans
WHERE flashloan_amount IS NOT NULL
    AND flashloan_token_symbol IN ('USDC', 'DAI', 'WETH', 'USDT')
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY flashloan_token_symbol
ORDER BY median DESC;
```

{% enddocs %}

{% docs ez_lending_premium_amount_unadj %}

## Premium Amount Unadjusted
The raw fee amount charged for the flash loan.

### Details
- **Type**: `NUMERIC`
- **Purpose**: Flash loan fee before decimal adjustment
- **Calculation**: Typically 0.05-0.09% of borrowed amount

### Usage Examples
```sql
-- Raw premium analysis
SELECT 
    platform,
    flashloan_token_symbol,
    AVG(premium_amount_unadj::FLOAT / NULLIF(flashloan_amount_unadj::FLOAT, 0) * 100) AS avg_fee_rate_pct,
    COUNT(*) AS sample_count
FROM <blockchain_name>.defi.ez_lending_flashloans
WHERE premium_amount_unadj > 0
    AND flashloan_amount_unadj > 0
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY 1, 2
ORDER BY 1, 3 DESC;
```

{% enddocs %}

{% docs ez_lending_premium_amount %}

## Premium Amount
The decimal-adjusted fee paid for the flash loan.

### Details
- **Type**: `NUMERIC`
- **Purpose**: Cost of borrowing via flash loan
- **Standards**: Usually fixed percentage per protocol

### Usage Examples
```sql
-- Flash loan fee comparison across platforms
SELECT 
    platform,
    AVG(premium_amount / NULLIF(flashloan_amount, 0) * 100) AS avg_fee_rate_pct,
    MIN(premium_amount / NULLIF(flashloan_amount, 0) * 100) AS min_fee_rate_pct,
    MAX(premium_amount / NULLIF(flashloan_amount, 0) * 100) AS max_fee_rate_pct,
    SUM(premium_amount) AS total_fees_collected,
    COUNT(*) AS flashloan_count
FROM <blockchain_name>.defi.ez_lending_flashloans
WHERE flashloan_amount > 0
    AND premium_amount > 0
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY platform
ORDER BY avg_fee_rate_pct;
```

{% enddocs %}

{% docs ez_lending_premium_amount_usd %}

## Premium Amount USD
The USD value of the flash loan fee.

### Details
- **Type**: `NUMERIC`
- **Purpose**: Fee revenue in dollar terms
- **Impact**: Direct protocol revenue

### Usage Examples
```sql
-- Daily flash loan fee revenue
SELECT 
    DATE_TRUNC('day', block_timestamp) AS date,
    platform,
    SUM(premium_amount_usd) AS daily_fee_revenue_usd,
    COUNT(*) AS flashloan_count,
    AVG(premium_amount_usd) AS avg_fee_per_loan_usd
FROM <blockchain_name>.defi.ez_lending_flashloans
WHERE premium_amount_usd IS NOT NULL
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;

-- Fee revenue by token
SELECT 
    flashloan_token_symbol,
    SUM(premium_amount_usd) AS total_fees_usd,
    SUM(flashloan_amount_usd) AS total_volume_usd,
    SUM(premium_amount_usd) / NULLIF(SUM(flashloan_amount_usd), 0) * 100 AS effective_fee_rate_pct,
    COUNT(*) AS loan_count
FROM <blockchain_name>.defi.ez_lending_flashloans
WHERE block_timestamp >= CURRENT_DATE - 90
    AND premium_amount_usd IS NOT NULL
    AND flashloan_token_symbol IS NOT NULL
GROUP BY flashloan_token_symbol
ORDER BY total_fees_usd DESC;
```

{% enddocs %}

{% docs ez_lending_collateral_asset %}

## Collateral Asset
The token contract address used as collateral in a liquidation.

### Details
- **Type**: `VARCHAR(42)`
- **Purpose**: Identifies the asset seized in liquidation
- **Risk**: Higher volatility assets have lower collateral factors

### Usage Examples
```sql
-- Most liquidated collateral assets
SELECT 
    collateral_asset,
    collateral_token_symbol,
    COUNT(*) AS liquidation_count,
    SUM(amount_usd) AS total_liquidated_usd,
    AVG(amount_usd) AS avg_liquidation_size_usd,
    COUNT(DISTINCT borrower) AS unique_borrowers_liquidated
FROM <blockchain_name>.defi.ez_lending_liquidations
WHERE collateral_asset IS NOT NULL
    AND amount_usd IS NOT NULL
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY 1, 2
ORDER BY total_liquidated_usd DESC;

-- Collateral risk analysis
WITH liquidation_rates AS (
    SELECT 
        collateral_asset,
        collateral_token_symbol,
        COUNT(DISTINCT borrower) AS liquidated_borrowers,
        SUM(amount_usd) AS total_liquidated_usd
    FROM <blockchain_name>.defi.ez_lending_liquidations
    WHERE block_timestamp >= CURRENT_DATE - 90
        AND collateral_asset IS NOT NULL
    GROUP BY 1, 2
),
total_deposits AS (
    SELECT 
        token_address AS collateral_asset,
        token_symbol AS collateral_token_symbol,
        COUNT(DISTINCT depositor) AS total_depositors,
        SUM(amount_usd) AS total_deposited_usd
    FROM <blockchain_name>.defi.ez_lending_deposits
    WHERE block_timestamp >= CURRENT_DATE - 90
        AND token_address IS NOT NULL
    GROUP BY 1, 2
)
SELECT 
    d.collateral_token_symbol,
    d.total_depositors,
    COALESCE(l.liquidated_borrowers, 0) AS liquidated_borrowers,
    COALESCE(l.liquidated_borrowers, 0) * 100.0 / d.total_depositors AS liquidation_rate_pct,
    d.total_deposited_usd,
    COALESCE(l.total_liquidated_usd, 0) AS total_liquidated_usd
FROM total_deposits d
LEFT JOIN liquidation_rates l 
    ON d.collateral_asset = l.collateral_asset
WHERE d.total_depositors > 100
ORDER BY liquidation_rate_pct DESC;
```

{% enddocs %}

{% docs ez_lending_collateral_token_symbol %}

## Collateral Token Symbol
The symbol of the asset used as collateral in liquidations.

### Details
- **Type**: `VARCHAR`
- **Purpose**: Human-readable collateral identifier
- **Common values**: `'WETH'`, `'WBTC'`, `'LINK'`, `'UNI'`, `'AAVE'`

### Usage Examples
```sql
-- Collateral preference in liquidations
SELECT 
    collateral_token_symbol,
    debt_token_symbol,
    COUNT(*) AS liquidation_pairs,
    SUM(amount_usd) AS total_collateral_seized_usd,
    SUM(debt_to_cover_amount_usd) AS total_debt_covered_usd,
    AVG((amount_usd - debt_to_cover_amount_usd) / NULLIF(debt_to_cover_amount_usd, 0) * 100) AS avg_liquidation_bonus_pct
FROM <blockchain_name>.defi.ez_lending_liquidations
WHERE collateral_token_symbol IS NOT NULL
    AND debt_token_symbol IS NOT NULL
    AND amount_usd IS NOT NULL
    AND debt_to_cover_amount_usd IS NOT NULL
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY 1, 2
ORDER BY liquidation_pairs DESC
LIMIT 50;
```

{% enddocs %}

{% docs ez_lending_debt_asset %}

## Debt Asset
The token contract address that was borrowed and is being repaid in liquidation.

### Details
- **Type**: `VARCHAR(42)`
- **Purpose**: Identifies the liability being covered
- **Pattern**: Often stablecoins due to borrowing preferences

### Usage Examples
```sql
-- Most common debt assets in liquidations
SELECT 
    debt_asset,
    debt_token_symbol,
    COUNT(*) AS liquidation_count,
    SUM(amount_usd) AS total_debt_liquidated_usd,
    COUNT(DISTINCT borrower) AS unique_borrowers,
    AVG(amount_usd) AS avg_debt_size_usd
FROM <blockchain_name>.defi.ez_lending_liquidations
WHERE debt_asset IS NOT NULL
    AND amount_usd IS NOT NULL
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY 1, 2
ORDER BY total_debt_liquidated_usd DESC;
```

{% enddocs %}

{% docs ez_lending_debt_token_symbol %}

## Debt Token Symbol
The symbol of the borrowed asset being repaid in liquidation.

### Details
- **Type**: `VARCHAR`
- **Common values**: `'USDC'`, `'USDT'`, `'DAI'`, `'WETH'`
- **Purpose**: Identifies borrowed asset type

### Usage Examples
```sql
-- Debt composition in liquidations
SELECT 
    debt_token_symbol,
    COUNT(*) AS liquidation_count,
    SUM(amount_usd) AS total_debt_usd,
    AVG(amount_usd) AS avg_debt_size,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount_usd) AS median_debt_size,
    COUNT(DISTINCT platform) AS platforms_affected
FROM <blockchain_name>.defi.ez_lending_liquidations
WHERE debt_token_symbol IS NOT NULL
    AND amount_usd IS NOT NULL
    AND block_timestamp >= CURRENT_DATE - 90
GROUP BY debt_token_symbol
ORDER BY total_debt_usd DESC;

-- Liquidation cascade risk (same debt asset)
WITH hourly_liquidations AS (
    SELECT 
        DATE_TRUNC('hour', block_timestamp) AS hour,
        debt_token_symbol,
        COUNT(*) AS liquidation_count,
        SUM(amount_usd) AS hourly_liquidation_volume
    FROM <blockchain_name>.defi.ez_lending_liquidations
    WHERE debt_token_symbol IS NOT NULL
        AND amount_usd IS NOT NULL
        AND block_timestamp >= CURRENT_DATE - 30
    GROUP BY 1, 2
)
SELECT 
    debt_token_symbol,
    MAX(liquidation_count) AS max_hourly_liquidations,
    MAX(hourly_liquidation_volume) AS max_hourly_volume_usd,
    AVG(liquidation_count) AS avg_hourly_liquidations,
    STDDEV(liquidation_count) AS liquidation_volatility
FROM hourly_liquidations
GROUP BY debt_token_symbol
HAVING MAX(liquidation_count) > 10
ORDER BY max_hourly_volume_usd DESC;
```

{% enddocs %}

{% docs ez_lending_amount_unadj %}

## Amount Unadjusted
The raw amount of tokens borrowed or repaid without decimal adjustment.

### Details
- **Type**: `NUMERIC`
- **Usage**: Raw blockchain value
- **Relationship**: `amount = amount_unadj / 10^decimals`

### Usage Examples 
```sql
SELECT 
    amount_unadj,
    amount,
    decimals
FROM <blockchain_name>.defi.ez_lending_borrows
WHERE amount_unadj IS NOT NULL;
```

{% enddocs %}

{% docs ez_lending_payer %}

## Payer
The address that paid the loan or deposit.

### Details
- **Type**: `VARCHAR(42)`
- **Purpose**: Identifies the payer address

{% enddocs %}