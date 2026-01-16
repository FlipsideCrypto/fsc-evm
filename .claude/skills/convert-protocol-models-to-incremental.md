# Skill: Convert Protocol Models to Incremental

This skill documents the process for converting protocol models from views to incremental models in fsc-evm. This pattern is used for curated protocol models (e.g., Aave, Compound, etc.) that need efficient incremental updates.

## Overview

Protocol models should use incremental materialization with the `delete+insert` strategy. This allows for efficient updates by only processing new/modified data while maintaining data integrity through unique key constraints.

**Key Requirements:**
1. `{{ config(...) }}` block MUST be at the very top of the model file
2. Models must include `modified_timestamp` column for incremental filtering
3. Use `CURATED_LOOKBACK_HOURS` and `CURATED_LOOKBACK_DAYS` variables for consistency
4. Macros must accept incremental parameters to support both full-refresh and incremental runs

---

## Critical Rule: Config Block Position

**THE CONFIG BLOCK MUST BE THE FIRST LINE OF THE MODEL FILE.**

dbt processes the config block before any output. If `{{ log_model_details() }}` or any other Jinja statement that produces output appears before the config block, dbt will NOT recognize the incremental settings and will default to `view` materialization.

### CORRECT Pattern:
```sql
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'aave', 'flashloan_fees', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Protocol-specific addresses #}
{% set pool_address = vars.PROTOCOL_AAVE_V3_POOL_ETHEREUM %}

{{ some_macro(...) }}
```

### WRONG Pattern (will create VIEW instead of incremental):
```sql
{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    ...
) }}
```

---

## Step 1: Update Macros for Incremental Support

If the protocol uses macros, update them to accept incremental parameters.

### Required Macro Parameters:
- `is_incremental_run` - Boolean to enable incremental filtering
- `lookback_hours` - Hours to look back for modified_timestamp (default: from vars)
- `lookback_days` - Days to look back as safety net (default: from vars)

### Required Output Columns:
- `modified_timestamp` - For incremental filtering
- `block_number` - For ordering and debugging

### Example Macro Pattern:

```sql
{% macro protocol_revenue_macro(chain, protocol, is_incremental_run=false, lookback_hours='48', lookback_days='7') %}
WITH base AS (
    SELECT
        block_number
        , block_timestamp
        , token_address
        , amount
        , modified_timestamp
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE contract_address = LOWER('{{ contract_address }}')
        AND event_name = 'SomeEvent'
    {% if is_incremental_run %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ lookback_hours }} hours'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ lookback_days }} days'
    {% endif %}
)
SELECT
    block_timestamp::date AS date
    , '{{ chain }}' AS chain
    , '{{ protocol }}' AS protocol
    , token_address
    , SUM(amount) AS amount_nominal
    , MAX(block_number) AS block_number
    , MAX(modified_timestamp) AS modified_timestamp
FROM base
GROUP BY 1, 2, 3, 4
{% endmacro %}
```

### Incremental Filtering Logic:

The dual-condition pattern ensures safe incremental updates:

```sql
{% if is_incremental_run %}
AND modified_timestamp >= (
    SELECT MAX(modified_timestamp) - INTERVAL '{{ lookback_hours }} hours'
    FROM {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ lookback_days }} days'
{% endif %}
```

- First condition: Look back from the last modified_timestamp in the target table
- Second condition: Safety net to prevent processing very old data on fresh tables

---

## Step 2: Create/Update Model Files

### Standard Model Config Block:

```sql
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', '{protocol}', '{metric_type}', 'curated']
) }}
```

### Config Options:

| Option | Value | Description |
|--------|-------|-------------|
| `materialized` | `'incremental'` | Always incremental for protocol models |
| `incremental_strategy` | `'delete+insert'` | Delete existing rows by unique_key, then insert |
| `unique_key` | `['date', 'token_address']` | Columns that uniquely identify rows (varies by model) |
| `cluster_by` | `['date']` | Clustering key for query performance |
| `tags` | `[...]` | Tags for model selection and organization |

### Common Unique Key Patterns:

- Daily aggregates: `['date', 'token_address']`
- Daily by protocol: `['date', 'protocol', 'token_address']`
- Transaction-level: `['tx_hash', 'event_index']`
- Hourly aggregates: `['hour', 'token_address']`

---

## Step 3: Model File Template

### For Models Using Macros:

```sql
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'protocol_name', 'metric_type', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Protocol-specific addresses #}
{% set pool_address = vars.PROTOCOL_SOME_ADDRESS %}

{{ protocol_macro(
    'ethereum',
    'Protocol Name',
    pool_address,
    is_incremental(),
    vars.CURATED_LOOKBACK_HOURS,
    vars.CURATED_LOOKBACK_DAYS
) }}
```

### For Models with Inline SQL:

```sql
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'protocol_name', 'metric_type', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Protocol-specific addresses #}
{% set contract_address = vars.PROTOCOL_SOME_ADDRESS %}

WITH
base AS (
    SELECT
        block_number
        , block_timestamp
        , token_address
        , amount
        , modified_timestamp
    FROM {{ ref('core__ez_token_transfers') }}
    WHERE contract_address = LOWER('{{ contract_address }}')
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}
)
SELECT
    block_timestamp::date AS date
    , 'ethereum' AS chain
    , token_address
    , SUM(amount) AS amount_nominal
    , MAX(block_number) AS block_number
    , MAX(modified_timestamp) AS modified_timestamp
FROM base
GROUP BY 1, 2, 3
```

---

## Step 4: Testing the Models

### Push Changes to fsc-evm:

```bash
cd /Users/mattromano/Desktop/repos/fsc-evm
git add .
git commit -m "feat: convert protocol models to incremental"
git push origin {branch-name}
```

### Update and Test in Project Repo:

```bash
cd /Users/mattromano/Desktop/repos/ethereum-models
rm -f package-lock.yml && dbt clean && dbt deps

# Test a single model with full-refresh first
dbt run -s silver_protocols__protocol_model_name --full-refresh

# Verify it created an incremental table (not a view)
# Output should show: "created sql incremental model"
```

### Verify Materialization Type:

After running, the dbt output should show:
```
1 of 1 OK created sql incremental model ETHEREUM_DEV.silver_protocols.protocol_model_name
```

If it shows `created sql table model` or `created sql view`, the config block is not at the top of the file.

---

## Step 5: Validation Queries

### Check Table Type in Snowflake:

```sql
SHOW TABLES LIKE 'protocol_model_name' IN SCHEMA ETHEREUM_DEV.silver_protocols;
-- Look at the "kind" column: should be "TABLE" not "VIEW"
```

### Verify Incremental Logic:

```sql
-- Check the date range of data
SELECT MIN(date), MAX(date), COUNT(*)
FROM ETHEREUM_DEV.silver_protocols.protocol_model_name;

-- Check modified_timestamp distribution
SELECT DATE_TRUNC('hour', modified_timestamp) as hour, COUNT(*)
FROM ETHEREUM_DEV.silver_protocols.protocol_model_name
GROUP BY 1
ORDER BY 1 DESC
LIMIT 10;
```

---

## Common Issues and Solutions

### Issue: Model Creates as VIEW Instead of Incremental

**Cause:** Config block is not at the very top of the file.

**Solution:** Move `{{ config(...) }}` to line 1, before any `{% set %}` statements or `{{ }}` output.

### Issue: "modified_timestamp column not found"

**Cause:** Source table or macro doesn't include modified_timestamp.

**Solution:** Add `modified_timestamp` to all SELECT statements and ensure it's included in the final output.

### Issue: Incremental Run Doesn't Pick Up New Data

**Cause:** The lookback window is too small or modified_timestamp is not being updated.

**Solution:** Check CURATED_LOOKBACK_HOURS and CURATED_LOOKBACK_DAYS values. Consider extending the lookback window.

### Issue: Duplicate Key Errors

**Cause:** The unique_key combination doesn't uniquely identify rows.

**Solution:** Add more columns to unique_key or aggregate data differently.

---

## File Structure Reference

Protocol models are typically organized as:

```
models/curated_package/protocols/{protocol}/
├── raw/
│   ├── {chain}/
│   │   ├── silver_protocols__{protocol}_{version}_{chain}_{metric}.sql
│   │   └── ...
│   └── silver_protocols__{protocol}_{metric}.sql  (chain-agnostic)
└── gold/
    └── (aggregated/final models)
```

Macros are stored in:
```
macros/curated_package/protocols/{protocol}/
└── {protocol}_macros.sql
```

---

## Checklist

### Before Converting:

- [ ] Identify all model files to convert
- [ ] Review existing macro structure
- [ ] Understand the unique key requirements for each model

### Macro Updates:

- [ ] Add `is_incremental_run` parameter
- [ ] Add `lookback_hours` and `lookback_days` parameters
- [ ] Add `modified_timestamp` to output columns
- [ ] Add `block_number` to output columns
- [ ] Add incremental filtering logic with dual conditions

### Model File Updates:

- [ ] Move `{{ config(...) }}` to line 1 (CRITICAL)
- [ ] Set `materialized = 'incremental'`
- [ ] Set `incremental_strategy = 'delete+insert'`
- [ ] Define appropriate `unique_key`
- [ ] Set `cluster_by` for performance
- [ ] Add appropriate tags
- [ ] Pass `is_incremental()` to macros
- [ ] Pass `vars.CURATED_LOOKBACK_HOURS` and `vars.CURATED_LOOKBACK_DAYS`

### Testing:

- [ ] Push changes to fsc-evm remote branch
- [ ] Run `make cleanup_time` in project repo
- [ ] Run model with `--full-refresh`
- [ ] Verify output shows "incremental model" (not "view")
- [ ] Run incremental update (without --full-refresh)
- [ ] Validate data in Snowflake
