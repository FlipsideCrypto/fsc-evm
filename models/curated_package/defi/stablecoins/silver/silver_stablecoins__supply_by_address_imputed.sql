{# Get variables #}
{% set vars = return_vars() %}
{# Log configuration details #}
{{ log_model_details() }}
-- depends_on: {{ ref('price__ez_asset_metadata') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ["stablecoins_supply_by_address_imputed_id"],
    cluster_by = ['block_date'],
    post_hook = [ "{{ unverify_stablecoins() }}", "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address, contract_address)" ],
    tags = ['silver','defi','stablecoins','heal','curated']
) }}

WITH base_supply AS (

    SELECT
        block_date,
        address,
        contract_address,
        balance,
        modified_timestamp
    FROM
        {{ ref('silver_stablecoins__supply_by_address') }}

{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),

{% if is_incremental() %}
-- Find earliest date needing reprocessing per address+contract (handles historical corrections/replays)
min_base_supply AS (
    SELECT
        MIN(block_date) AS min_base_supply_date,
        address,
        contract_address
    FROM
        base_supply
    GROUP BY
        address,
        contract_address
),
-- Pull complete history from earliest affected date forward for impacted pairs (needed for re-imputation)
incremental_supply AS (
    SELECT
        s.block_date,
        s.address,
        s.contract_address,
        balance,
        modified_timestamp,
        FALSE AS is_imputed
    FROM
        {{ ref('silver_stablecoins__supply_by_address') }}
        s
        INNER JOIN min_base_supply m
        ON s.address = m.address
        AND s.contract_address = m.contract_address
        AND s.block_date >= m.min_base_supply_date
),
-- Lookup of address+contract pairs being updated this run (used to filter unchanged pairs)
base_supply_list AS (
    SELECT
        address,
        contract_address,
        COUNT(1)
    FROM
        base_supply
    GROUP BY
        ALL
),
trailing_gaps AS (
    SELECT
        address,
        contract_address,
        MAX(block_date) AS gap_start_date
    FROM
        {{ this }}
    WHERE
        modified_timestamp >= SYSDATE() :: DATE - 7 -- only look back 7 days for efficiency
    GROUP BY
        address,
        contract_address
    HAVING
        gap_start_date < SYSDATE() :: DATE - 1
),
-- Get all existing records for pairs with gaps (from gap start forward)
existing_supply AS (
    SELECT
        t.block_date,
        t.address,
        t.contract_address,
        t.balance,
        t.modified_timestamp,
        t.is_imputed
    FROM
        {{ this }}
        t
        INNER JOIN trailing_gaps g
        ON t.address = g.address
        AND t.contract_address = g.contract_address
        AND t.block_date >= g.gap_start_date
        LEFT JOIN base_supply_list b
        ON t.address = b.address
        AND t.contract_address = b.contract_address
    WHERE
        b.address IS NULL -- Exclude pairs already in base_supply
),
{% endif %}

all_supply AS (

{% if is_incremental() %}
SELECT
    *
FROM
    incremental_supply
UNION ALL
SELECT
    *
FROM
    existing_supply
{% else %}
SELECT
    *
FROM
    base_supply
{% endif %}),
-- Identify unique address+contract pairs and their first balance date
address_contract_pairs AS (
    SELECT
        address,
        contract_address,
        MIN(block_date) AS min_balance_date
    FROM
        all_supply
    GROUP BY
        address,
        contract_address
),
-- Create a date spine for all dates between the minimum balance date and the current date - 1 day,
-- Balances are recorded using the last block from the previous day
date_spine AS (
    SELECT
        date_day
    FROM
        {{ source(
            'crosschain_gold',
            'dim_dates'
        ) }}
    WHERE
        date_day < SYSDATE() :: DATE
        AND date_day >= (
            SELECT
                MIN(block_date)
            FROM
                all_supply
        )
),
-- Generate one row per date per address+contract pair (filtered by min_balance_date)
date_address_contract_spine AS (
    SELECT
        d.date_day AS block_date,
        p.address,
        p.contract_address
    FROM
        date_spine d
        INNER JOIN address_contract_pairs p
        ON d.date_day >= p.min_balance_date
),
-- Forward-fill missing balances to create one row per date per address+contract pair, even in cases where no balance new balance is recorded
filled_balances AS (
    SELECT
        s.block_date,
        s.address,
        s.contract_address,
        COALESCE(
            A.balance,
            LAST_VALUE(
                A.balance ignore nulls
            ) over (
                PARTITION BY s.address,
                s.contract_address
                ORDER BY
                    s.block_date rows BETWEEN unbounded preceding
                    AND CURRENT ROW
            )
        ) AS balance,

{% if is_incremental() %}
-- If incremental, use the imputed flag from the existing record, otherwise use the balance to determine if it is imputed
COALESCE(
    A.is_imputed,
    TRUE
) AS is_imputed,
{% else %}
    CASE
        WHEN A.balance IS NULL THEN TRUE
        ELSE FALSE
    END AS is_imputed,
{% endif %}

A.modified_timestamp
FROM
    date_address_contract_spine s
    LEFT JOIN all_supply A
    ON s.block_date = A.block_date
    AND s.address = A.address
    AND s.contract_address = A.contract_address
)
SELECT
    block_date,
    address,
    contract_address,
    balance,
    is_imputed,
    SYSDATE() AS modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_date','address','contract_address']) }} AS stablecoins_supply_by_address_imputed_id
FROM
    filled_balances
WHERE
    NOT IFF(
        balance = 0
        AND is_imputed,
        TRUE,
        FALSE
    ) -- Exclude pairs with zero balance AND imputed flag, to avoid imputing indefinitely
