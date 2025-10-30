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
    post_hook = '{{ unverify_stablecoins() }}',
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
        {{ ref('silver__stablecoins_supply_by_address') }}

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
        modified_timestamp
    FROM
        {{ ref('silver__stablecoins_supply_by_address') }}
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
-- Get latest balance for unchanged address+contract pairs to preserve continuity
existing_supply AS (
    SELECT
        block_date,
        address,
        contract_address,
        balance,
        modified_timestamp,
        is_imputed
    FROM
        {{ this }}
        t
        LEFT JOIN base_supply_list b USING (
            address,
            contract_address
        )
    WHERE
        b.address IS NULL qualify ROW_NUMBER() over (
            PARTITION BY address,
            contract_address
            ORDER BY
                block_date DESC
        ) = 1
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