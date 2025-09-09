{% docs ez_balances_erc20_table_doc %}

## What

This table tracks ERC20 token balance changes at the transaction level by capturing pre- and post-transaction states from contract storage slots. It uses state tracer data to show exactly how each address's token balance changed during transaction execution for verified ERC20 tokens, including decimal adjustments and USD valuations, where available, for comprehensive token balance analysis. This data set includes both successful and failed transactions, as state may change regardless.

## Key Use Cases

- Tracking ERC20 token balance changes at transaction granularity
- Analyzing token balance impacts of DeFi interactions and trades
- Monitoring large token balance changes and whale activity
- Calculating precise token balance evolution over time
- Identifying addresses with significant token holdings
- Debugging smart contract effects on token balances
- Analyzing token distribution and concentration metrics

## Important Relationships

- **Join with fact_transactions**: Use `tx_hash` for transaction context
- **Join with fact_blocks**: Use `block_number` for block metadata
- **Join with dim_labels**: Use `address` for entity identification
- **Join with dim_contracts**: Use `contract_address` for token contract details
- **Join with ez_token_transfers**: Compare balance changes to transfer events
- **Join with ez_prices_hourly**: USD valuations already included but can be refreshed
- **Join with ez_balances_native**: Compare with native token balance changes

## Commonly-used Fields

- `address`: The account whose token balance changed
- `contract_address`: The ERC20 token contract address
- `symbol`: Token symbol (USDC, WETH, etc.)
- `pre_balance`: Token balance before the transaction
- `post_balance`: Token balance after the transaction
- `net_balance`: The change in token balance (post - pre)
- `pre_balance_usd` / `post_balance_usd`: USD values at time of transaction
- `decimals`: Number of decimal places for the token
- `tx_hash`: Transaction that caused the balance change

## Sample queries

**Daily ERC20 Token Balance Changes**
```sql
SELECT 
    DATE_TRUNC('day', block_timestamp) AS day,
    symbol,
    COUNT(*) AS balance_changes,
    COUNT(DISTINCT address) AS unique_holders,
    SUM(ABS(net_balance)) AS total_balance_moved,
    SUM(net_balance) AS net_balance_change
FROM <blockchain_name>.balances.ez_balances_erc20
WHERE block_timestamp >= CURRENT_DATE - 30
    AND net_balance != 0
    AND symbol IS NOT NULL
GROUP BY 1, 2
ORDER BY 1 DESC, total_balance_moved DESC;
```

**Token Holder Balance Evolution**
```sql
-- Track how a specific address's token balances changed over time
SELECT 
    block_timestamp,
    tx_hash,
    contract_address,
    symbol,
    pre_balance,
    post_balance,
    net_balance,
    pre_balance_usd,
    post_balance_usd
FROM <blockchain_name>.balances.ez_balances_erc20
WHERE address = LOWER('0x1234567890123456789012345678901234567890')
    AND block_timestamp >= CURRENT_DATE - 30
    AND net_balance != 0
ORDER BY block_timestamp DESC;
```

**DeFi Protocol Token Impact Analysis**
```sql
-- Analyze how DeFi interactions affect token balances
SELECT 
    t.to_address AS protocol_address,
    b.symbol,
    COUNT(*) AS balance_changes,
    COUNT(DISTINCT b.address) AS unique_users,
    SUM(CASE WHEN b.net_balance > 0 THEN b.net_balance ELSE 0 END) AS total_gains,
    SUM(CASE WHEN b.net_balance < 0 THEN ABS(b.net_balance) ELSE 0 END) AS total_losses
FROM <blockchain_name>.balances.ez_balances_erc20 b
JOIN <blockchain_name>.core.fact_transactions t USING (tx_hash)
WHERE b.net_balance != 0
    AND t.to_address IN (SELECT address FROM dim_contracts)
    AND b.block_timestamp >= CURRENT_DATE - 7
    AND b.symbol IS NOT NULL
GROUP BY 1, 2
HAVING COUNT(*) > 10
ORDER BY total_gains + total_losses DESC
LIMIT 50;
```

**Token Balance Distribution by Token**
```sql
-- Analyze current token holders and their balances
SELECT 
    contract_address,
    symbol,
    COUNT(DISTINCT address) AS holder_count,
    SUM(post_balance) AS total_supply_tracked,
    AVG(post_balance) AS avg_balance,
    MAX(post_balance) AS max_balance
FROM <blockchain_name>.balances.ez_balances_erc20
WHERE block_timestamp >= CURRENT_DATE - 1
    AND post_balance > 0
    AND symbol IS NOT NULL
GROUP BY 1, 2
ORDER BY holder_count DESC
LIMIT 50;
```

{% enddocs %}

{% docs ez_balances_erc20_contract_address %}

The ERC20 token contract address whose balance changed in this transaction.

Example: '0xa0b86a33e6eb88b4d81b15e4e60c8a5b776e3b7a'

{% enddocs %}

{% docs ez_balances_erc20_symbol %}

The token symbol for the ERC20 token.

Example: 'USDC'

{% enddocs %}

{% docs ez_balances_erc20_address %}

The account address whose token balance changed in this transaction.

Example: '0x1234567890123456789012345678901234567890'

{% enddocs %}

{% docs ez_balances_erc20_slot_number %}

The storage slot number used to track balances for this ERC20 token contract.

Example: 0

{% enddocs %}

{% docs ez_balances_erc20_decimals %}

Number of decimal places for the token, used for proper decimal adjustment.

Example: 6

{% enddocs %}

{% docs ez_balances_erc20_tx_succeeded %}

Boolean indicator of whether the transaction that caused this balance change was successful.

Example: true

{% enddocs %}

{% docs ez_balances_erc20_pre_balance %}

Token balance before the transaction execution, decimal adjusted to standard units.

Example: 1000.50

{% enddocs %}

{% docs ez_balances_erc20_post_balance %}

Token balance after the transaction execution, decimal adjusted to standard units.

Example: 750.25

{% enddocs %}

{% docs ez_balances_erc20_net_balance %}

The change in token balance (post_balance - pre_balance).

Example: -250.25

{% enddocs %}

{% docs ez_balances_erc20_pre_balance_usd %}

USD value of the pre-transaction token balance at the time of the transaction.

Example: 1000.50

{% enddocs %}

{% docs ez_balances_erc20_post_balance_usd %}

USD value of the post-transaction token balance at the time of the transaction.

Example: 750.25

{% enddocs %}

{% docs ez_balances_erc20_pre_balance_precise %}

Token balance before transaction, decimal adjusted, returned as a string to preserve precision.

Example: '1000.500000'

{% enddocs %}

{% docs ez_balances_erc20_post_balance_precise %}

Token balance after transaction, decimal adjusted, returned as a string to preserve precision.

Example: '750.250000'

{% enddocs %}

{% docs ez_balances_erc20_pre_balance_raw %}

Token balance before transaction in smallest unit, no decimal adjustment.

Example: 1000500000

{% enddocs %}

{% docs ez_balances_erc20_post_balance_raw %}

Token balance after transaction in smallest unit, no decimal adjustment.

Example: 750250000

{% enddocs %}

{% docs ez_balances_erc20_net_balance_raw %}

The change in token balance in smallest unit.

Example: -250250000

{% enddocs %}

{% docs ez_balances_erc20_pre_balance_hex %}

Hexadecimal representation of the pre-transaction balance as stored in the contract's storage slot.

Example: '0x3b9aca00'

{% enddocs %}

{% docs ez_balances_erc20_post_balance_hex %}

Hexadecimal representation of the post-transaction balance as stored in the contract's storage slot.

Example: '0x2cb417800'

{% enddocs %}