{% docs dim_contract_abis_table_doc %}

## What

This table contains Application Binary Interfaces (ABIs) for smart contracts deployed on EVM blockchains. ABIs define the contract's functions, events, and data structures, enabling the decoding of raw blockchain data into human-readable format.

## Key Use Cases

- Decoding raw event logs into human-readable format
- Identifying contract functions and their parameters
- Enabling interaction with smart contracts programmatically
- Analyzing contract patterns and implementations across chains
- Supporting automated contract verification and bytecode matching

## Important Relationships

- **Powers ez_decoded_event_logs**: ABIs enable event decoding
- **Join with dim_contracts**: Use `contract_address` for contract metadata
- **Enables fact_decoded_event_logs**: Raw to decoded transformation

## Commonly-used Fields

- `contract_address`: The contract's blockchain address
- `abi`: The contract's Application Binary Interface in JSON format
- `abi_source`: The origin of the ABI data (explorer verified, user submitted, bytecode matched)
- `bytecode`: The compiled contract code deployed on-chain
- `created_timestamp`: When the ABI was added to the database

## Sample queries

**Find Contracts Without ABIs**
```sql
-- Identify popular contracts needing ABIs
WITH contract_activity AS (
    SELECT 
        contract_address,
        COUNT(*) AS event_count
    FROM <blockchain_name>.core.fact_event_logs
    WHERE block_timestamp >= CURRENT_DATE - 7
    GROUP BY 1
)
SELECT 
    ca.contract_address,
    c.name AS contract_name,
    ca.event_count,
    c.created_block_timestamp
FROM contract_activity ca
LEFT JOIN <blockchain_name>.core.dim_contract_abis abi ON ca.contract_address = abi.contract_address
LEFT JOIN <blockchain_name>.core.dim_contracts c ON ca.contract_address = c.address
WHERE abi.abi IS NULL
    OR abi.abi = '[]'
ORDER BY ca.event_count DESC
LIMIT 100;
```

**Analyze ABI Functions and Events**
```sql
-- Extract event signatures from ABIs
WITH abi_events AS (
    SELECT 
        contract_address,
        abi_source,
        f.value:name::string AS event_name,
        f.value:type::string AS entry_type
    FROM <blockchain_name>.core.dim_contract_abis,
    LATERAL FLATTEN(input => PARSE_JSON(abi)) f
    WHERE f.value:type::string = 'event'
        AND abi IS NOT NULL
)
SELECT 
    event_name,
    COUNT(DISTINCT contract_address) AS contracts_with_event,
    ARRAY_AGG(DISTINCT abi_source) AS sources
FROM abi_events
GROUP BY 1
ORDER BY 2 DESC
LIMIT 50;
```

**Bytecode Matching Effectiveness**
```sql
-- Analyze bytecode matching success
SELECT 
    DATE_TRUNC('week', created_timestamp) AS week,
    COUNT(CASE WHEN abi_source = 'bytecode_matched' THEN 1 END) AS bytecode_matched,
    COUNT(CASE WHEN abi_source = 'user_submitted' THEN 1 END) AS user_submitted,
    COUNT(CASE WHEN abi_source LIKE '%explorer%' THEN 1 END) AS explorer_verified,
    COUNT(*) AS total_new_abis
FROM <blockchain_name>.core.dim_contract_abis
WHERE created_timestamp >= CURRENT_DATE - 90
GROUP BY 1
ORDER BY 1 DESC;
```

**Common Contract Patterns**
```sql
-- Find contracts sharing bytecode (proxy patterns, clones)
WITH bytecode_groups AS (
    SELECT 
        bytecode,
        COUNT(DISTINCT contract_address) AS contract_count,
        ARRAY_AGG(DISTINCT contract_address) AS contracts,
        MAX(abi) AS sample_abi
    FROM <blockchain_name>.core.dim_contract_abis
    WHERE bytecode IS NOT NULL
        AND LENGTH(bytecode) > 100  -- Exclude minimal contracts
    GROUP BY 1
    HAVING COUNT(DISTINCT contract_address) > 5
)
SELECT 
    contract_count,
    ARRAY_SIZE(contracts) AS unique_addresses,
    LEFT(bytecode, 20) || '...' AS bytecode_prefix,
    CASE 
        WHEN sample_abi LIKE '%proxy%' THEN 'Likely Proxy'
        WHEN sample_abi LIKE '%clone%' THEN 'Likely Clone'
        ELSE 'Standard Pattern'
    END AS pattern_type
FROM bytecode_groups
ORDER BY contract_count DESC
LIMIT 20;
```

{% enddocs %}

{% docs dim_contract_abis_abi %}

The contract's Application Binary Interface in JSON format, containing function and event definitions that enable interaction with the smart contract.

Example: '[{"name":"transfer","type":"function","inputs":[{"name":"to","type":"address"},{"name":"value","type":"uint256"}],"outputs":[{"name":"","type":"bool"}]}]'

{% enddocs %}

{% docs dim_contract_abis_abi_source %}

The origin of the ABI data, indicating trust level and collection method.

Example: 'etherscan'

{% enddocs %}

{% docs dim_contract_abis_bytecode %}

The compiled contract code deployed on-chain, used for bytecode matching and identifying identical contracts.

Example: '0x608060405234801561001057600080fd5b50...'

{% enddocs %}