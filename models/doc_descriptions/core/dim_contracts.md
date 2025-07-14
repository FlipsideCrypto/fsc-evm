{% docs dim_contracts_table_doc %}

## What

This table provides comprehensive metadata for all smart contracts deployed on EVM blockchains. It includes contract names, symbols, decimals, and deployment details read directly from the blockchain.

## Key Use Cases

- Identifying contracts by name, symbol, or address
- Understanding token properties (decimals, symbols)
- Tracking contract deployment patterns and trends
- Finding contracts deployed by specific factories or deployers
- Filtering protocol-specific data across other tables

## Important Relationships

- **Join with fact_transactions**: Use `address = to_address` for contract interactions
- **Join with fact_event_logs**: Use `address = contract_address` for contract events
- **Join with ez_token_transfers**: Use `address = contract_address` for token movements

## Commonly-used Fields

- `address`: The deployed contract's blockchain address (lowercase)
- `name`: Human-readable contract name from the name() function
- `symbol`: Token/contract symbol from the symbol() function
- `decimals`: Number of decimal places for token amounts
- `creator_address`: Address that deployed this contract
- `created_block_timestamp`: When the contract was created

## Sample queries

**Find All Uniswap V3 Pool Contracts**

```sql
SELECT 
    address,
    name,
    created_block_number,
    created_block_timestamp,
    creator_address
FROM <blockchain_name>.core.dim_contracts
WHERE creator_address = LOWER('0x1F98431c8aD98523631AE4a59f267346ea31F984') -- Uniswap V3 Factory
ORDER BY created_block_number DESC
LIMIT 100;
```

**Analyze Contract Deployment Trends**

```sql
SELECT 
    DATE_TRUNC('week', created_block_timestamp) AS week,
    COUNT(*) AS contracts_deployed,
    COUNT(DISTINCT creator_address) AS unique_deployers
FROM <blockchain_name>.core.dim_contracts
WHERE created_block_timestamp >= CURRENT_DATE - 90
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;
```

{% enddocs %}

{% docs dim_contracts_created_block_number %}

Block number when contract was created.

Example: 17500000

{% enddocs %}

{% docs dim_contracts_created_block_timestamp %}

Timestamp when contract was created.

Example: 2023-06-15 14:30:00.000

{% enddocs %}

{% docs dim_contracts_address %}

Unique identifier - the deployed contract's blockchain address.

Example: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'

{% enddocs %}

{% docs dim_contracts_name %}

Human-readable contract name from the name() function.

Example: 'USD Coin'

{% enddocs %}

{% docs dim_contracts_symbol %}

Token/contract symbol from the symbol() function.

Example: 'USDC'

{% enddocs %}

{% docs dim_contracts_creator_address %}

Address that deployed this contract (transaction from_address).

Example: '0x95ba4cf87d6723ad9c0db21737d862be80e93911'

{% enddocs %}

{% docs dim_contracts_decimals %}

Number of decimal places for token amounts, read directly from the contract code.

Example: 6

{% enddocs %}

{% docs dim_contracts_created_tx_hash %}

Transaction hash of the contract deployment.

Example: '0x4f01db1f857e711af502ad6fa8b5b3ccd9e36b5f8c8a7b2c1d3e4f5a6b7c8d9e'

{% enddocs %}