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

WITH bridge_vault_list AS (

    SELECT
        DISTINCT bridge_address AS address
    FROM
        {{ ref('defi__ez_bridge_activity') }}
    UNION
    SELECT
        vault_address AS address
    FROM
        {{ ref('silver_stablecoins__bridge_vault_seed') }}
    WHERE
        chain = '{{ vars.GLOBAL_PROJECT_NAME }}'
),
base_supply AS (
    SELECT
        block_date,
        address,
        contract_address,
        balance,
        modified_timestamp
    FROM
        {{ ref('silver__stablecoins_supply_by_address') }}
        INNER JOIN bridge_vault_list USING (address)
    WHERE
        1 = 1

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),

{% if is_incremental() %}
-- get the min date by contract and address for new modified timestamp entries. so new reg incremental runs + if any replays
min_base_supply AS (
    SELECT
        MIN(block_date) AS min_base_supply_date,
        address,
        contract_address
    FROM
        base_supply
    GROUP BY
        ALL
),
-- get all possible dates & balance for new modified timestamp entries - both reg inc runs + replays if any
base_supply_reloads AS (
    SELECT
        s.block_date,
        s.address,
        s.contract_address,
        balance,
        modified_timestamp
    FROM
        {{ ref('silver__stablecoins_supply_by_address') }}
        s
        INNER JOIN bridge_vault_list USING (address)
        INNER JOIN min_base_supply m
        ON s.address = m.address
        AND s.contract_address = m.contract_address
        AND s.block_date >= m.min_base_supply_date
),
{# min_latest_date AS (
-- Find the minimum of the latest block_dates across all address/contract pairs
-- This is the earliest date we need to pull to ensure all pairs have recent history
SELECT
    MIN(max_date) AS min_of_latest_dates
FROM
    (
        SELECT
            MAX(block_date) AS max_date
        FROM
            {{ this }}
        GROUP BY
            address,
            contract_address
    )
),
existing_supply AS (
    -- Pull all records >= min_of_latest_dates to provide seed balances for gap-filling
    SELECT
        block_date,
        address,
        contract_address,
        balance,
        modified_timestamp
    FROM
        {{ this }}
    WHERE
        block_date >= (
            SELECT
                min_of_latest_dates
            FROM
                min_latest_date
        )
),
#}
{% endif %}

all_supply AS (

{% if is_incremental() %}
SELECT
    *
FROM
    base_supply_reloads
{% else %}
SELECT
    *
FROM
    base_supply
{% endif %}),
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
                MIN(min_balance_date)
            FROM
                address_contract_pairs
        )
),
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
    SYSDATE() AS modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_date','address','contract_address']) }} AS stablecoins_supply_by_address_imputed_id
FROM
    filled_balances
