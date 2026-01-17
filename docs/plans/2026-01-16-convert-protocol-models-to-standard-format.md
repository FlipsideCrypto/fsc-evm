# Convert Protocol Models to Standard Format

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Convert all 253 protocol models from the legacy `fact_*`/`dim_*` naming and directory structure to the standard `silver_protocols__` schema format with proper incremental configuration and YAML documentation.

**Architecture:** Rename files to `silver_protocols__{table_name}.sql`, flatten directory structure from `protocols/{protocol}/raw/{chain}/` to `protocols/{protocol}/`, add incremental config blocks, and create YAML test files for each model.

**Tech Stack:** dbt, Jinja2, Snowflake

---

## Summary

| Protocol | Files Converted | Status |
|----------|-----------------|--------|
| renzo_protocol | 1 | ✅ Complete |
| uniswap | 2 | ✅ Complete |
| lido | 4 | ✅ Complete |
| eigenlayer | 6 | ✅ Complete |
| aerodrome | 7 | ✅ Complete |
| convex | 7 | ✅ Complete |
| liquity | 9 | ✅ Complete |
| balancer | 28 | ✅ Complete |
| goldfinch | 30 | ✅ Complete |
| maker | 42 | ✅ Complete |
| chainlink | 56 | ✅ Complete |
| aave | 61 | ✅ Complete |
| **Total** | **253** | ✅ **All Complete** |

**Completed:** 2026-01-16

---

## Standard Format Reference

### File Naming Convention
```
# OLD (wrong)
fact_aave_v3_arbitrum_deposits_borrows_lender_revenue.sql
silver_protocols__aave_v3_ethereum_deposits_borrows_lender_revenue.sql  # wrong location

# NEW (correct)
silver_protocols__aave_v3_arbitrum_deposits_borrows_lender_revenue.sql
```

### Directory Structure
```
# OLD (wrong)
models/curated_package/protocols/aave/raw/arbitrum/fact_*.sql
models/curated_package/protocols/aave/raw/ethereum/silver_protocols__*.sql

# NEW (correct)
models/curated_package/protocols/aave/silver_protocols__*.sql
models/curated_package/protocols/aave/_aave__models.yml
```

### SQL Template (Target Format)
```sql
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token_address'],  -- adjust per model
    cluster_by = ['date'],
    tags = ['silver_protocols', '{protocol}', '{metric_type}', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Protocol-specific addresses #}
{% set pool_address = vars.PROTOCOL_AAVE_V3_POOL_ARBITRUM %}
{% set collector_address = vars.PROTOCOL_AAVE_COLLECTOR_ARBITRUM %}

{{ aave_deposits_borrows_lender_revenue(
    'arbitrum',
    'AAVE V3',
    pool_address,
    collector_address,
    'raw_aave_v3_arbitrum_rpc_data',
    is_incremental(),
    vars.CURATED_LOOKBACK_HOURS,
    vars.CURATED_LOOKBACK_DAYS
) }}
```

### YAML Template
```yaml
version: 2
models:
  - name: silver_protocols__{model_name}
    description: "{Description of what this model captures}"
    tests:
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns:
              - date
              - token_address
    columns:
      - name: DATE
        tests:
          - not_null
      - name: TOKEN_ADDRESS
        tests:
          - not_null
      - name: MODIFIED_TIMESTAMP
        tests:
          - not_null
      - name: INSERTED_TIMESTAMP
        tests:
          - not_null
```

---

## Task 1: Aave Protocol (61 files)

This is the largest and most complex protocol with models across multiple chains and versions (V2, V3).

### Files
- Source: `models/curated_package/protocols/aave/raw/*/`
- Target: `models/curated_package/protocols/aave/`
- YAML: `models/curated_package/protocols/aave/_aave__models.yml`

### Step 1.1: Move and rename Ethereum V3 models (already have config, just need relocation)

Files to move from `aave/raw/ethereum/` to `aave/`:
- `silver_protocols__aave_v3_ethereum_deposits_borrows_lender_revenue.sql`
- `silver_protocols__aave_v3_ethereum_ecosystem_incentives.sql`
- `silver_protocols__aave_v3_ethereum_flashloan_fees.sql`
- `silver_protocols__aave_v3_ethereum_liquidation_revenue.sql`
- `silver_protocols__aave_v3_ethereum_reserve_factor_revenue.sql`
- `silver_protocols__aave_v2_ethereum_deposits_borrows_lender_revenue.sql`
- `silver_protocols__aave_v2_ethereum_ecosystem_incentives.sql`
- `silver_protocols__aave_v2_ethereum_flashloan_fees.sql`
- `silver_protocols__aave_v2_ethereum_liquidation_revenue.sql`
- `silver_protocols__aave_v2_ethereum_reserve_factor_revenue.sql`
- `silver_protocols__aave_dao_balancer_trading_fees.sql`
- `silver_protocols__aave_dao_safety_incentives.sql`
- `silver_protocols__aave_gho_treasury_revenue.sql`

Run:
```bash
mv models/curated_package/protocols/aave/raw/ethereum/silver_protocols__*.sql models/curated_package/protocols/aave/
```

### Step 1.2: Move and rename root-level Aave models

Files to move from `aave/raw/` to `aave/`:
- `silver_protocols__aave_aavura_treasury.sql`
- `silver_protocols__aave_ecosystem_reserve.sql`
- `silver_protocols__aave_safety_module.sql`
- `silver_protocols__aave_v2_collector.sql`

Run:
```bash
mv models/curated_package/protocols/aave/raw/silver_protocols__*.sql models/curated_package/protocols/aave/
```

### Step 1.3: Convert Arbitrum V3 models

For each file in `aave/raw/arbitrum/`:

**1.3.1: fact_aave_v3_arbitrum_deposits_borrows_lender_revenue.sql**

Read current file, then create new file at `aave/silver_protocols__aave_v3_arbitrum_deposits_borrows_lender_revenue.sql`:

```sql
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'aave', 'deposits_borrows_lender_revenue', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Protocol-specific addresses #}
{% set pool_address = vars.PROTOCOL_AAVE_V3_POOL_ARBITRUM %}
{% set collector_address = vars.PROTOCOL_AAVE_COLLECTOR_ARBITRUM %}

{{ aave_deposits_borrows_lender_revenue(
    'arbitrum',
    'AAVE V3',
    pool_address,
    collector_address,
    'raw_aave_v3_arbitrum_rpc_data',
    is_incremental(),
    vars.CURATED_LOOKBACK_HOURS,
    vars.CURATED_LOOKBACK_DAYS
) }}
```

Delete old file: `rm models/curated_package/protocols/aave/raw/arbitrum/fact_aave_v3_arbitrum_deposits_borrows_lender_revenue.sql`

**1.3.2: fact_aave_v3_arbitrum_ecosystem_incentives.sql** - Same pattern
**1.3.3: fact_aave_v3_arbitrum_flashloan_fees.sql** - Same pattern
**1.3.4: fact_aave_v3_arbitrum_liquidation_revenue.sql** - Same pattern
**1.3.5: fact_aave_v3_arbitrum_reserve_factor_revenue.sql** - Same pattern

### Step 1.4: Convert Avalanche V2 models (5 files)
### Step 1.5: Convert Avalanche V3 models (5 files)
### Step 1.6: Convert Base V3 models (5 files)
### Step 1.7: Convert BSC V3 models (4 files)
### Step 1.8: Convert Gnosis V3 models (5 files)
### Step 1.9: Convert Optimism V3 models (5 files)
### Step 1.10: Convert Polygon V2 models (5 files)
### Step 1.11: Convert Polygon V3 models (5 files)

### Step 1.12: Create Aave YAML file

Create `models/curated_package/protocols/aave/_aave__models.yml` with all model definitions.

### Step 1.13: Remove empty directories

```bash
rm -rf models/curated_package/protocols/aave/raw/
```

### Step 1.14: Verify Aave models compile

```bash
dbt compile -m tag:aave
```

### Step 1.15: Commit Aave changes

```bash
git add models/curated_package/protocols/aave/
git commit -m "refactor: convert Aave protocol models to standard format"
```

---

## Task 2: Aerodrome Protocol (7 files)

### Files
- Source: `models/curated_package/protocols/aerodrome/raw/`
- Target: `models/curated_package/protocols/aerodrome/`

### Step 2.1: Convert each model
- `fact_aerodrome_supply_data.sql` → `silver_protocols__aerodrome_supply_data.sql`
- `fact_aerodrome_third_party_incentives.sql` → `silver_protocols__aerodrome_third_party_incentives.sql`
- `fact_aerodrome_v1_swaps.sql` → `silver_protocols__aerodrome_v1_swaps.sql`
- `fact_aerodrome_v1_tvl.sql` → `silver_protocols__aerodrome_v1_tvl.sql`
- `fact_aerodrome_v2_pools.sql` → `silver_protocols__aerodrome_v2_pools.sql`
- `fact_aerodrome_v2_swaps.sql` → `silver_protocols__aerodrome_v2_swaps.sql`
- `fact_aerodrome_v2_tvl.sql` → `silver_protocols__aerodrome_v2_tvl.sql`

### Step 2.2: Create Aerodrome YAML file
### Step 2.3: Remove empty directories
### Step 2.4: Verify and commit

---

## Task 3: Balancer Protocol (28 files)

### Files
- Source: `models/curated_package/protocols/balancer/raw/`
- Target: `models/curated_package/protocols/balancer/`

Complex structure with V1, V2 across multiple chains (Ethereum, Arbitrum, Gnosis, Polygon).

### Step 3.1: Convert V1 models (4 files)
### Step 3.2: Convert V2 Arbitrum models (5 files)
### Step 3.3: Convert V2 Ethereum models (5 files)
### Step 3.4: Convert V2 Gnosis models (5 files)
### Step 3.5: Convert V2 Polygon models (5 files)
### Step 3.6: Convert root-level models (4 files)
### Step 3.7: Create Balancer YAML file
### Step 3.8: Remove empty directories
### Step 3.9: Verify and commit

---

## Task 4: Chainlink Protocol (56 files)

### Files
- Source: `models/curated_package/protocols/chainlink/raw/`
- Target: `models/curated_package/protocols/chainlink/`

Models across Arbitrum, Avalanche, Base, BSC, Ethereum, Optimism, Polygon.

### Step 4.1-4.7: Convert each chain's models
### Step 4.8: Create Chainlink YAML file
### Step 4.9: Remove empty directories
### Step 4.10: Verify and commit

---

## Task 5: Convex Protocol (7 files)

### Step 5.1: Convert models
### Step 5.2: Create YAML file
### Step 5.3: Remove empty directories
### Step 5.4: Verify and commit

---

## Task 6: Eigenlayer Protocol (6 files)

### Step 6.1: Convert models
### Step 6.2: Create YAML file
### Step 6.3: Remove empty directories
### Step 6.4: Verify and commit

---

## Task 7: Goldfinch Protocol (30 files)

### Step 7.1: Convert models (including dim_* files)
### Step 7.2: Create YAML file
### Step 7.3: Remove empty directories
### Step 7.4: Verify and commit

---

## Task 8: Lido Protocol (4 files)

### Step 8.1: Convert models
### Step 8.2: Create YAML file
### Step 8.3: Remove empty directories
### Step 8.4: Verify and commit

---

## Task 9: Liquity Protocol (9 files)

### Step 9.1: Convert models
### Step 9.2: Create YAML file
### Step 9.3: Remove empty directories
### Step 9.4: Verify and commit

---

## Task 10: Maker Protocol (42 files)

### Step 10.1: Convert models (including dim_* files)
### Step 10.2: Create YAML file
### Step 10.3: Remove empty directories
### Step 10.4: Verify and commit

---

## Task 11: Renzo Protocol (1 file)

### Step 11.1: Convert model
### Step 11.2: Create YAML file
### Step 11.3: Remove empty directories
### Step 11.4: Verify and commit

---

## Task 12: Uniswap Protocol (2 files)

### Step 12.1: Convert models
### Step 12.2: Create YAML file
### Step 12.3: Remove empty directories
### Step 12.4: Verify and commit

---

## Task 13: Final Verification

### Step 13.1: Run full compile
```bash
dbt compile -m tag:silver_protocols
```

### Step 13.2: Verify no orphaned files
```bash
find models/curated_package/protocols -name "fact_*.sql" -o -name "dim_*.sql"
# Should return nothing
```

### Step 13.3: Verify directory structure is flat
```bash
find models/curated_package/protocols -type d -name "raw"
# Should return nothing
```

### Step 13.4: Create final PR
```bash
git add .
git commit -m "refactor: convert all protocol models to standard format

- Renamed 253 models from fact_*/dim_* to silver_protocols__*
- Flattened directory structure (removed raw/ and chain subdirs)
- Added incremental config blocks with delete+insert strategy
- Created YAML test files for each protocol
- Added is_incremental() and lookback params to macro calls"
```

---

## Conversion Checklist Per Model

For each model file:
- [ ] Read original file to understand macro and variables used
- [ ] Create new file with `silver_protocols__` prefix in flattened location
- [ ] Add config block at TOP of file with:
  - `materialized = 'incremental'`
  - `incremental_strategy = 'delete+insert'`
  - `unique_key` (model-specific)
  - `cluster_by` (usually `['date']`)
  - `tags` array
- [ ] Keep variable setup (`return_vars()`, `log_model_details()`)
- [ ] Add `is_incremental()`, `vars.CURATED_LOOKBACK_HOURS`, `vars.CURATED_LOOKBACK_DAYS` to macro call
- [ ] Delete original file
- [ ] Add model to protocol's YAML file
