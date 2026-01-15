# Skill: Testing fsc-evm Changes

This skill documents the process for testing changes made to fsc-evm, the central EVM data package. Since fsc-evm is a shared package consumed by project repos, testing requires setting up a downstream project repo to validate changes.

## Overview

fsc-evm is a central package managed with git tags for versioning. To test changes:
1. Push your fsc-evm branch to remote
2. Open a project repo that consumes fsc-evm (e.g., `ethereum-models`, `arbitrum-models`)
3. Create a test branch in the project repo
4. Point `packages.yml` to your fsc-evm branch
5. Run `make cleanup_time` to reload dependencies
6. Run dbt commands to validate

---

## Prerequisites

- fsc-evm branch pushed to remote (changes must be synced)
- Access to a project repo in `/Users/mattromano/Desktop/repos/` (e.g., `ethereum-models`, `arbitrum-models`)
- snowsql CLI configured
- dbt installed and configured for dev target

---

## Step 1: Ensure fsc-evm Branch is Pushed

Your fsc-evm branch must be on the remote for project repos to pull it.

```bash
cd /Users/mattromano/Desktop/repos/fsc-evm
git status  # Confirm you're on your feature branch
git push -u origin {branch-name}
# Example: git push -u origin DAT2-237/cc/curate-fluid-dex
```

---

## Step 2: Open the Project Repo

Choose the appropriate project repo based on what you're testing:
- ethereum-models - For Ethereum mainnet changes
- arbitrum-models - For Arbitrum changes
- Other chain-specific repos as needed

```bash
cd /Users/mattromano/Desktop/repos/{project-repo}
# Example: cd /Users/mattromano/Desktop/repos/ethereum-models
```

---

## Step 3: Create a Test Branch in Project Repo

Important: Create a new branch in the project repo for testing. This keeps package.yml changes isolated.

```bash
git checkout main
git pull origin main
git checkout -b test/{fsc-evm-branch-name}
# Example: git checkout -b test/DAT2-237-curate-fluid-dex
```

---

## Step 4: Edit packages.yml

Modify packages.yml to point to your fsc-evm branch instead of a tagged version.

Before (tagged version):
```yaml
packages:
  - git: "https://github.com/FlipsideCrypto/fsc-evm.git"
    revision: "v1.2.3"  # or whatever tag is current
```

After (your branch):
```yaml
packages:
  - git: "https://github.com/FlipsideCrypto/fsc-evm.git"
    revision: "DAT2-237/cc/curate-fluid-dex"  # your branch name
```

---

## Step 5: Run make cleanup_time

This command handles the full dependency reload:
- Removes package-lock.yml
- Runs dbt clean
- Runs dbt deps

```bash
make cleanup_time
```

Note: This may take a moment as it pulls your fsc-evm branch fresh.

---

## Step 6: Run dbt Commands to Validate

Now you can test your fsc-evm changes via the project repo. Always run against dev.

### Example Validation Workflow for DEX Curation

```bash
# 1. Build the silver-level models
dbt run --select silver_dex__fluid_v1_pools silver_dex__fluid_v1_swaps silver_dex__fluid_v1_pool_actions --target dev

# 2. Run a full refresh for the protocol in complete models using CURATED_FR_MODELS
# IMPORTANT: The value must match the CTE name in the complete model (e.g., "fluid_v1" not "fluid")
# Run all complete models in one command to avoid multiple dbt startups
dbt run --select silver_dex__complete_dex_liquidity_pools silver_dex__complete_dex_swaps silver_dex__complete_dex_liquidity_pool_actions --target dev --vars '{"CURATED_FR_MODELS": ["fluid_v1"]}'

# 3. Run any dbt tests
dbt test --select silver_dex__fluid_v1_swaps --target dev
```

**IMPORTANT - CURATED_FR_MODELS naming:**
- The value must match the CTE name in the complete model, not just the protocol name
- Check the complete model SQL for the correct name: `grep -n "CURATED_FR_MODELS" <model>.sql`
- Common pattern: `{protocol}_{version}` (e.g., fluid_v1, uniswap_v3, curve_v1)

**Ask before running dbt models - especially for larger runs.**

---

## Step 7: Validate Against External Sources

After building, query the results and compare to external data sources. Use the same date range for both sources.

### Using DeFiLlama Data in Snowflake (Preferred)

DeFiLlama DEX volume data is available in `external.defillama.FACT_DEX_VOLUME`:

```sql
-- Compare monthly totals: Flipside vs DeFiLlama
WITH flipside AS (
    SELECT ROUND(SUM(amount_in_usd), 0) as fs_volume
    FROM ethereum_dev.silver_dex.complete_dex_swaps
    WHERE platform = 'fluid-v1'
      AND block_timestamp::DATE BETWEEN '2025-12-01' AND '2025-12-31'
),
defillama AS (
    SELECT SUM(volume) as dl_volume
    FROM external.defillama.FACT_DEX_VOLUME
    WHERE LOWER(protocol) = 'fluid dex'
      AND LOWER(chain) = 'ethereum'
      AND date BETWEEN '2025-12-01' AND '2025-12-31'
)
SELECT
    d.dl_volume as defillama,
    f.fs_volume as flipside,
    ROUND((f.fs_volume / d.dl_volume) * 100, 1) as pct_match
FROM defillama d, flipside f;
```

Notes:
- Daily comparisons may show variance due to timezone differences in date bucketing
- Monthly totals should be within 5% for a good match
- DeFiLlama protocol names may differ from ours (e.g., "fluid dex" vs "fluid-v1")

### Check Available Chains in DeFiLlama

```sql
-- Find all chains for a protocol
SELECT chain, MIN(date) as first_date, MAX(date) as last_date, ROUND(SUM(volume), 0) as total_volume
FROM external.defillama.FACT_DEX_VOLUME
WHERE LOWER(protocol) LIKE '%fluid%'
GROUP BY 1
ORDER BY 4 DESC;
```

### Other Validation Sources

- https://defillama.com/dexs (web UI)
- Protocol's official analytics dashboard
- Block explorer transaction counts

---

## Quick Verification (Not Best Practice)

For small, quick checks you can edit files directly in the project repo's `target/` folder after `dbt deps` imports fsc-evm. This lets you test minor tweaks without pushing changes.

Caveats:
- Changes in `target/` are NOT included in the PR
- Useful only for quick debugging/iteration
- Must still implement proper changes in fsc-evm and re-test via the standard process

---

## After fsc-evm PR is Merged

Once testing is complete and the fsc-evm PR is merged:

### 1. Tag the new fsc-evm version

In fsc-evm repo, create a new semantic version tag:

```bash
cd /Users/mattromano/Desktop/repos/fsc-evm
git checkout main
git pull origin main
make new_repo_tag  # Follow prompts for semantic versioning
```

Note: Austin typically handles the merge and tagging.

### 2. Update project repo branch with new tag

Go back to the project repo test branch and update packages.yml to use the new tag:

```yaml
packages:
  - git: "https://github.com/FlipsideCrypto/fsc-evm.git"
    revision: "v1.2.4"  # New tagged version
```

Then run `make cleanup_time` again to pull the tagged version.

### 3. PR the project repo changes

The project repo branch can now be PR'd to update the fsc-evm version in that project.

---

## Multi-Chain Deployment

Many protocols operate on multiple chains. After validating on the primary chain, deploy to other chains.

### Check Which Chains a Protocol Supports

```sql
-- Find all chains for a protocol in DeFiLlama
SELECT chain, ROUND(SUM(volume), 0) as total_volume
FROM external.defillama.FACT_DEX_VOLUME
WHERE LOWER(protocol) LIKE '%{protocol_name}%'
GROUP BY 1
ORDER BY 2 DESC;
```

### Deployment Process for Additional Chains

1. Use the same fsc-evm branch - models are chain-agnostic if using `return_vars()`
2. Open the chain's project repo (e.g., `arbitrum-models`, `base-models`)
3. Create a test branch in that repo
4. Update `packages.yml` to point to your fsc-evm branch
5. Run `make cleanup_time`
6. Verify contract addresses in the chain's vars file:
   - Check `macros/global/variables/project_vars/{chain}_vars.sql`
   - Add factory addresses to `CURATED_DEFI_DEX_SWAPS_CONTRACT_MAPPING` if needed
7. Run and validate the models
8. Validate against DeFiLlama for each chain separately

Notes:
- Contract addresses may differ by chain - verify in Spellbook or protocol docs
- Some chains may not have all pool types
- Prioritize chains by volume (check DeFiLlama)

---

## Troubleshooting

### "Revision not found" error during dbt deps

- Ensure your fsc-evm branch is pushed to remote
- Check for typos in the branch name in packages.yml
- Try `git fetch origin` in the project repo

### Models not reflecting changes

- Run `make cleanup_time` again to ensure fresh pull
- Check that package-lock.yml was removed
- Verify you're importing from the correct fsc-evm path

### Query running too long

- Use `block_timestamp::DATE` filters (cluster key)
- Use `LIMIT` when exploring
- Cancel and notify if >30 seconds unexpectedly

---

## Checklist

### Testing Phase

- [ ] fsc-evm branch pushed to remote
- [ ] Project repo selected based on chain being tested
- [ ] Test branch created in project repo
- [ ] packages.yml updated to point to fsc-evm branch
- [ ] `make cleanup_time` completed successfully
- [ ] Silver models built and tested
- [ ] Complete models loaded with new protocol data (using CURATED_FR_MODELS)
- [ ] Output validated against external sources (DeFiLlama, etc.)

### Post-Merge Phase (after fsc-evm PR merged)

- [ ] fsc-evm tagged with new semantic version (`make new_repo_tag`)
- [ ] Project repo packages.yml updated to new tag
- [ ] `make cleanup_time` run with tagged version
- [ ] Project repo PR created to update fsc-evm version
