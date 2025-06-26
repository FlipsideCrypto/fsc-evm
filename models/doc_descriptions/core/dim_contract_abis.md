{% docs dim_contract_abis_table_doc %}

## Table: dim_contract_abis

This table contains Application Binary Interfaces (ABIs) for smart contracts deployed on EVM blockchains. ABIs define the contract's functions, events, and data structures, enabling the decoding of raw blockchain data into human-readable format. This table powers the decoded event logs and function calls throughout Flipside's data infrastructure.

### Key Features:
- **Multi-Source Collection**: ABIs from block explorers, community submissions, and bytecode matching
- **Comprehensive Coverage**: Major protocols, tokens, and verified contracts
- **Continuous Updates**: New ABIs added daily through automated and manual processes
- **Quality Validation**: ABIs verified for accuracy and completeness

### ABI Sources (Priority Order):
1. **Block Explorer Verified**: Contracts verified on Etherscan/similar (highest trust)
2. **Community Submitted**: User-provided ABIs via [ABI Requestor](https://science.flipsidecrypto.xyz/abi-requestor/)
3. **Bytecode Matched**: ABIs inferred from matching deployed bytecode

### Important Relationships:
- **Powers ez_decoded_event_logs**: ABIs enable event decoding
- **Join with dim_contracts**: Use `contract_address` for contract metadata
- **Enables fact_decoded_event_logs**: Raw to decoded transformation

### Sample Queries:

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

### Submitting ABIs:
1. Visit the [ABI Requestor](https://science.flipsidecrypto.xyz/abi-requestor/)
2. Provide the contract address and network
3. Paste the contract ABI in JSON format
4. ABIs are typically processed within 24-48 hours

### Quality Indicators:
- **Verified Source**: Block explorer verified contracts are most reliable
- **Function Count**: More functions/events = more comprehensive ABI
- **Recent Updates**: Newer ABIs may include latest contract changes

{% enddocs %}

{% docs dim_contract_abis_abi %}

The contract's Application Binary Interface in JSON format.

**Structure**: JSON array of function and event definitions
**Contents**:
- Function signatures and parameters
- Event definitions with indexed parameters
- Constructor and fallback functions
- Error definitions (Solidity 0.8.4+)

**Example Structure**:
```json
[
  {
    "name": "transfer",
    "type": "function",
    "inputs": [
      {"name": "to", "type": "address"},
      {"name": "value", "type": "uint256"}
    ],
    "outputs": [{"name": "", "type": "bool"}]
  },
  {
    "name": "Transfer",
    "type": "event",
    "inputs": [
      {"name": "from", "type": "address", "indexed": true},
      {"name": "to", "type": "address", "indexed": true},
      {"name": "value", "type": "uint256", "indexed": false}
    ]
  }
]
```

**Usage Pattern**:
```sql
-- Count functions and events in ABI
SELECT 
    contract_address,
    ARRAY_SIZE(PARSE_JSON(abi)) AS total_entries,
    SUM(CASE WHEN f.value:type = 'function' THEN 1 ELSE 0 END) AS function_count,
    SUM(CASE WHEN f.value:type = 'event' THEN 1 ELSE 0 END) AS event_count
FROM <blockchain_name>.core.dim_contract_abis,
LATERAL FLATTEN(input => PARSE_JSON(abi)) f
WHERE abi IS NOT NULL
GROUP BY 1, abi
LIMIT 100;
```

{% enddocs %}

{% docs dim_contract_abis_abi_source %}

The origin of the ABI data, indicating trust level and collection method.

**Values**:
- `etherscan` / `{explorer}_verified`: Verified via block explorer (highest trust)
- `user_submitted`: Community provided via ABI requestor
- `bytecode_matched`: Inferred from matching bytecode patterns
- `manual_entry`: Flipside team additions

**Trust Hierarchy**:
1. Explorer verified - Cryptographically verified source code
2. User submitted - Community verified, widely used
3. Bytecode matched - Automated inference, verify critical functions

**Analysis Example**:
```sql
-- ABI source distribution
SELECT 
    abi_source,
    COUNT(*) AS contract_count,
    SUM(CASE WHEN LENGTH(abi) > 1000 THEN 1 ELSE 0 END) AS complex_contracts
FROM <blockchain_name>.core.dim_contract_abis
WHERE abi IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC;
```

{% enddocs %}

{% docs dim_contract_abis_bytecode %}

The compiled contract code deployed on-chain, used for bytecode matching.

**Format**: Hex string (0x prefixed)
**Usage**:
- Identify identical contracts (clones, proxies)
- Match contracts with known ABIs
- Detect contract patterns and families

**Key Points**:
- Excludes constructor parameters
- Identifies implementation logic
- Same bytecode = same functionality

**Pattern Detection**:
```sql
-- Find common bytecode patterns
SELECT 
    LEFT(bytecode, 10) AS bytecode_prefix,
    COUNT(DISTINCT contract_address) AS instances,
    ARRAY_AGG(DISTINCT abi_source) AS sources,
    CASE 
        WHEN COUNT(DISTINCT contract_address) > 100 THEN 'Factory Pattern'
        WHEN COUNT(DISTINCT contract_address) > 10 THEN 'Common Implementation'
        ELSE 'Unique/Rare'
    END AS pattern_classification
FROM <blockchain_name>.core.dim_contract_abis
WHERE bytecode IS NOT NULL
    AND LENGTH(bytecode) > 100
GROUP BY 1
HAVING COUNT(DISTINCT contract_address) > 5
ORDER BY 2 DESC;
```

{% enddocs %}