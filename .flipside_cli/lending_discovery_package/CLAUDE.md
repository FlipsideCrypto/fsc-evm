# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **Flipside Curated Lending Models** repository containing dbt models that power the `{chain}.defi.ez_lending_*` tables. The models extract, transform, and standardize lending protocol data from raw blockchain event logs.

## Lending Models Architecture

### Directory Structure

```
lending/
├── gold/                          # Public-facing ez_lending views
│   ├── defi__ez_lending_deposits.sql
│   ├── defi__ez_lending_borrows.sql
│   ├── defi__ez_lending_repayments.sql
│   ├── defi__ez_lending_withdraws.sql
│   ├── defi__ez_lending_liquidations.sql
│   └── defi__ez_lending_flashloans.sql
└── silver/
    ├── complete_lending/          # Union of all protocol data
    │   ├── silver_lending__complete_lending_deposits.sql
    │   ├── silver_lending__complete_lending_borrows.sql
    │   └── ...
    └── protocols/                 # Protocol-specific models
        ├── aave/
        ├── compound_v2/
        ├── compound_v3/
        ├── euler/
        ├── fraxlend/
        ├── morpho/
        └── silo/
```

### Data Flow

1. **Raw Event Logs** (`core__fact_event_logs`) → Filter by `topics[0]` (event signature)
2. **Protocol Silver Models** → Parse event data, join with token metadata
3. **Complete Lending** → Union all protocols with standardized schema
4. **Gold Layer** → Clean public views

## How Protocols Are Identified

Each lending protocol emits specific events with unique `topics[0]` signatures (keccak256 hash of event signature).

### Currently Tracked Event Signatures

#### Aave (Deposits)
```sql
topics[0] IN (
  '0xde6857219544bb5b7746f48ed30be6386fefc61b2f864cacf559893bf50fd951',  -- Deposit (v1/v2)
  '0x2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61',  -- Supply (v3)
  '0xc12c57b1c73a2c3a2ea4613e9476abb3d8d146857aab7329e24243fb59710c82'   -- Supply variant
)
```

#### Aave (Borrows)
```sql
topics[0] IN (
  '0xc6a898309e823ee50bac64e45ca8adba6690e99e7841c45d754e2a38e9019d9b',  -- Borrow v1
  '0x1e77446728e5558aa1b7e81e0cdab9cc1b075ba893b740600c76a315c2caa553',  -- Borrow v2
  '0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0'   -- Borrow v3
)
```

#### Compound V2 (Deposits/Mint)
```sql
topics[0] IN (
  '0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f',  -- Mint
  '0xb4c03061fb5b7fed76389d5af8f2e0ddb09f8c70d1333abbb62582835e10accb',  -- Mint variant
  '0x2f00e3cdd69a77be7ed215ec7b2a36784dd158f921fca79ac29deffa353fe6ee'   -- Mint fork
)
```

#### Compound V2 (Borrows)
```sql
topics[0] IN (
  '0x13ed6866d4e1ee6da46f845c46d7e54120883d75c5ea9a2dacc1c4ca8984ab80',  -- Borrow
  '0x2dd79f4fccfd18c360ce7f9132f3621bf05eee18f995224badb32d17f172df73'   -- Borrow variant
)
```

#### Compound V3
```sql
-- Deposits
'0xfa56f7b24f17183d81894d3ac2ee654e3c26388d17a28dbd9549b8114304e1f4'  -- SupplyCollateral
'0xd1cf3d156d5f8f0d50f6c122ed609cec09d35c9b9fb3fff6ea0959134dae424e'  -- Supply base
```

#### Euler
```sql
'0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7'  -- Deposit
'0x0cd345140b9008a43f99a999a328ece572a0193e8c8bf5f5755585e6f293b85e'  -- NewMarket
```

#### Fraxlend
```sql
'0xa32435755c235de2976ed44a75a2f85cb01faf0c894f639fe0c32bb9455fea8f'  -- AddCollateral
```

#### Silo
```sql
'0xdd160bb401ec5b5e5ca443d41e8e7182f3fe72d70a04b9c0ba844483d212bcb5'  -- Deposit
'0x312a5e5e1079f5dda4e95dbbd0b908b291fd5b992ef22073643ab691572c5b52'  -- Borrow
```

#### Morpho (Trace-based, not event logs)
```sql
function_sig = '0xa99aad89'  -- supply
function_sig = '0x50d8cd4b'  -- borrow
```

## Adding a New Protocol

### Step 1: Identify Event Signatures

Find the protocol's event signatures by:
1. Check Etherscan for verified contract ABIs
2. Query `{chain}.silver.flat_event_abis` for known signatures
3. Analyze `{chain}.core.fact_event_logs` for the contract's events

### Step 2: Create Protocol Directory

```
silver/protocols/{protocol_name}/
├── silver_lending__{protocol}_deposits.sql
├── silver_lending__{protocol}_borrows.sql
├── silver_lending__{protocol}_repayments.sql
├── silver_lending__{protocol}_withdraws.sql
├── silver_lending__{protocol}_liquidations.sql
└── silver_lending__{protocol}_asset_details.sql  (optional)
```

### Step 3: Protocol Model Template

```sql
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated','{protocol}']
) }}

WITH events AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        topics,
        data,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        -- Extract indexed params from topics
        CONCAT('0x', SUBSTR(topics[1]::STRING, 27, 40)) AS param1_address,
        -- Extract non-indexed params from data
        utils.udf_hex_to_int(segmented_data[0]::STRING) AS param2_amount,
        modified_timestamp,
        CONCAT(tx_hash::STRING, '-', event_index::STRING) AS _log_id
    FROM {{ ref('core__fact_event_logs') }}
    WHERE topics[0]::STRING IN (
        '0x...'  -- Event signature(s)
    )
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
        FROM {{ this }}
    )
    {% endif %}
    AND tx_succeeded
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    param1_address AS protocol_market,
    token_address,
    amount_unadj,
    depositor,
    '{protocol}' AS protocol,
    'v1' AS version,
    '{protocol}-v1' AS platform,
    _log_id,
    modified_timestamp,
    'Deposit' AS event_name
FROM events
qualify(ROW_NUMBER() over(PARTITION BY _log_id ORDER BY modified_timestamp DESC)) = 1
```

### Step 4: Add to Complete Lending

Edit `silver_lending__complete_lending_{event_type}.sql`:
1. Add CTE for new protocol
2. Add to UNION ALL clause

## Key Tables

| Table | Purpose |
|-------|---------|
| `{chain}.core.fact_event_logs` | Raw event logs with topics[0] signatures |
| `{chain}.core.fact_traces` | Function call traces (for Morpho-style protocols) |
| `{chain}.silver.flat_event_abis` | Event signature → name mapping |
| `{chain}.core.dim_labels` | Contract labels (DeFi, lending, etc.) |
| `{chain}.core.dim_contracts` | Contract metadata (name, symbol, decimals) |
| `curated_contract_mapping` | Protocol-specific contract configurations |

## Common Patterns

### Extract Address from Topic
```sql
CONCAT('0x', SUBSTR(topics[1]::STRING, 27, 40)) AS address
```

### Extract Amount from Data
```sql
utils.udf_hex_to_int(segmented_data[0]::STRING)::INTEGER AS amount
```

### Parse Event Data
```sql
regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data
```

### Deduplication
```sql
qualify(ROW_NUMBER() over(PARTITION BY _log_id ORDER BY modified_timestamp DESC)) = 1
```

## Flipside CLI Reference

### Setup & Configuration

```bash
# Initialize CLI with API key
flipside init
# or
flipside config init

# Check current user/config
flipside whoami

# Update CLI to latest version
flipside update
```

### SQL Queries

Execute SQL directly against Flipside's data warehouse:

```bash
# Run inline SQL
flipside query "SELECT * FROM ethereum.core.fact_blocks LIMIT 10"

# Run from file
flipside query my_query.sql

# Output options
flipside query "SELECT ..." --format json           # JSON output (default: csv)
flipside query "SELECT ..." --output results.csv   # Custom output path
flipside query "SELECT ..." --preview              # Show preview table
flipside query "SELECT ..." --no-download          # Just get download URL
```

### Skills Management

Skills provide domain knowledge and tools to agents:

```bash
# List your deployed skills
flipside skills list

# Deploy/update a skill from YAML
flipside skills push my_skill.skill.yaml

# View skill details
flipside skills describe my_skill

# Delete a skill
flipside skills delete my_skill

# Browse Flipside's global skills catalog
flipside catalog skills list
```

### Agent Management

Agents are AI assistants that use skills:

```bash
# List your deployed agents
flipside agent list

# Deploy/update an agent from YAML
flipside agent push my_agent.agent.yaml

# View agent details
flipside agent describe my_agent

# Validate agent YAML without deploying
flipside agent validate my_agent.agent.yaml

# Delete an agent
flipside agent delete my_agent

# Browse Flipside's global agents catalog
flipside catalog agents list
```

### Running Agents

```bash
# Run agent with a single message
flipside agent run my_agent --message "Your question here"

# Run with verbose output (see tool calls)
flipside agent run my_agent --message "..." -v
```

### Interactive Chat

```bash
# Start interactive REPL
flipside chat

# Create a new chat session
flipside chat create

# List your chat sessions
flipside chat list

# Resume a previous session
flipside chat resume <session-id>

# Send a message to a session
flipside chat send-message <session-id> "Your message"

# View session messages
flipside chat list-messages <session-id>
```

### Catalog (Global Resources)

```bash
# List Flipside's global agents
flipside catalog agents list

# List Flipside's global skills
flipside catalog skills list

# List available tools
flipside catalog tools list
```

### Skill YAML Structure

```yaml
name: My Skill Name
slug: my_skill
description: What this skill does

tools:
  - run_sql_query      # Execute SQL queries
  - find_tables        # Search for tables
  - get_table_schema   # Get table columns/types

knowledge: |
  # Skill Knowledge

  This markdown content is provided to the agent when using this skill.
  Include:
  - Domain expertise
  - SQL query patterns
  - Table references
  - Best practices
```

### Agent YAML Structure

```yaml
name: my_agent
kind: chat                    # 'chat' for interactive, 'sub' for sub-agents
description: What this agent does

skills:
  - YourUsername/my_skill     # Reference skills with namespace
  - flipside/snowflake        # Or global Flipside skills

systemprompt: |
  You are an AI assistant that...

  ## Your Role
  ...

maxturns: 15                  # Optional: limit conversation turns

metadata:                     # Optional: custom metadata
  version: "1.0"
```

### Common Workflows

```bash
# Deploy skill + agent together
flipside skills push lending_discovery.skill.yaml
flipside agent push lending_discovery.agent.yaml

# Quick test
flipside agent run lending_discovery_agent --message "Test query"

# Interactive exploration
flipside chat
> /use lending_discovery_agent
> Find missing lending protocols on Ethereum
```

### Debugging

```bash
# Verbose mode shows all tool calls and responses
flipside agent run my_agent --message "..." -v

# View traces for a chat session
flipside chat traces <session-id>

# JSON output for programmatic use
flipside agent list -j
flipside query "SELECT 1" -j
```
