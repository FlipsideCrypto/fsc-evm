{% docs evm_contracts_table_doc %}

## Table: dim_contracts

This table provides comprehensive metadata for all smart contracts deployed on EVM blockchains. It includes contract names, symbols, decimals, and deployment details read directly from the blockchain. Essential for identifying contracts, understanding token properties, and filtering protocol-specific data.

### Key Features:
- **Contract Identification**: Names, symbols, and addresses for all deployed contracts
- **Deployment Info**: Creator addresses, creation transactions, and deployment blocks

### Data Sources:
- **On-chain Reading**: Direct contract queries for name(), symbol(), decimals()

### Important Relationships:
- **Join with fact_transactions**: Use `address = to_address` for contract interactions
- **Join with fact_event_logs**: Use `address = contract_address` for contract events
- **Join with ez_token_transfers**: Use `address = contract_address` for token movements

### Sample Queries:

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

### Performance Optimization:
- Use lowercase addresses for joins: `LOWER(address)`
- Filter by token_standard when searching specific contract types
- Add created_block_timestamp constraints for recent contracts
- Index on address, creator_address, and token_standard

{% enddocs %}

{% docs dim_contracts_created_block_number %}

Block number when contract was created.

**Usage**:
- Filter for recent contracts
- Track contract creation patterns
- Analyze deployment activity

{% enddocs %}

{% docs dim_contracts_created_block_timestamp %}

Timestamp when contract was created.

**Format**: TIMESTAMP_NTZ

**Usage**:
- Filter for recent contracts
- Track contract creation patterns
- Analyze deployment activity

{% enddocs %}

{% docs dim_contracts_address %}

Unique identifier - the deployed contract's blockchain address.

**Format**: Lowercase 42-character hex string (0x + 40 chars)
**Uniqueness**: Primary key - one row per contract address
**Normalization**: Always lowercase for consistent joins

**Join Examples**:

```sql
-- Find all transactions to a contract
SELECT * FROM <blockchain_name>.core.fact_transactions 
WHERE to_address = (SELECT address FROM <blockchain_name>.core.dim_contracts WHERE symbol = 'USDC');

-- Get contract metadata for events
SELECT e.*, c.name, c.symbol
FROM <blockchain_name>.core.fact_event_logs e
JOIN <blockchain_name>.core.dim_contracts c ON e.contract_address = c.address;
```

{% enddocs %}

{% docs dim_contracts_name %}

Human-readable contract name from the name() function.

**Usage**:
- Filter for specific contracts
- Identify contract types
- Analyze contract relationships

- "Uniswap V2: USDC-ETH"
- "OpenSea: Shared Storefront"
- "Wrapped Ether"

**NULL When**: Contract has no name function or unverified

**Important**:
- This is not the contract name, but the name of the contract as defined in the contract code.
- This is not unique, as multiple contracts can share the same name.

{% enddocs %}

{% docs dim_contracts_symbol %}

Token/contract symbol from the symbol() function.

**Common Examples**:
- "USDC", "USDT", "DAI" (stablecoins)
- "WETH", "WBTC" (wrapped assets)  
- "UNI-V2" (LP tokens)

**Uniqueness**: NOT unique - many tokens may share symbols
**NULL For**: Non-token contracts or contracts without symbol()

**Usage Pattern**:

```sql
-- Find all USD stablecoins
SELECT address, name, symbol, decimals
FROM <blockchain_name>.core.dim_contracts
WHERE symbol IN ('USDC', 'USDT', 'DAI', 'BUSD', 'TUSD');
```

{% enddocs %}

{% docs dim_contracts_creator_address %}

Address that deployed this contract (transaction from_address).

**Use Cases**:
- Identify factory patterns
- Track deployer activity
- Find related contracts
- Security analysis

**Factory Pattern Example**:

```sql
-- Find all Uniswap V2 pairs
SELECT COUNT(*) AS total_pairs
FROM <blockchain_name>.core.dim_contracts
WHERE creator_address = LOWER('0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f'); -- V2 Factory
```

{% enddocs %}

{% docs dim_contracts_decimals %}

Number of decimal places for token amounts, read directly from the contract code.

**Standard Values**:
- 18: Most ERC-20 tokens (ETH standard)
- 6: USDC, USDT (USD stablecoins)
- 8: WBTC (Bitcoin standard)
- 0: NFTs and some special tokens

**Usage in Calculations**:

```sql
-- Convert raw amounts to human-readable
SELECT 
    symbol,
    raw_amount / POWER(10, decimals) AS human_amount
FROM <blockchain_name>.core.ez_token_transfers t
JOIN <blockchain_name>.core.dim_contracts c ON t.contract_address = c.address
WHERE c.decimals IS NOT NULL;
```

{% enddocs %}

{% docs dim_contracts_created_tx_hash %}

Transaction hash of the contract deployment.

**Usage**:
- Link to deployment transaction details
- Find deployment costs
- Analyze deployment parameters

**Query Pattern**:

```sql
-- Get contract deployment details
SELECT 
    c.address,
    c.name,
    c.created_tx_hash,
    t.from_address AS deployer,
    t.gas_used * t.gas_price AS deployment_cost_wei,
    t.block_timestamp AS deployed_at
FROM <blockchain_name>.core.dim_contracts c
JOIN <blockchain_name>.core.fact_transactions t ON c.created_tx_hash = t.tx_hash
WHERE c.symbol = 'UNI';
```

{% enddocs %}