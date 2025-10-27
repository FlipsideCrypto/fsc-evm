{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('price__ez_asset_metadata') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ["stablecoins_supply_all_address_imputed_id"],
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
        {{ ref('silver__stablecoins_supply_all_address') }}
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
min_latest_date AS (
    -- Find the minimum of the latest block_dates across all address/contract pairs
    -- This is the earliest date we need to pull to ensure all pairs have recent history
    SELECT
        MIN(max_date) AS min_of_latest_dates
    FROM (
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
        block_date >= (SELECT min_of_latest_dates FROM min_latest_date)
),
{% endif %}
all_supply AS (
    SELECT * FROM base_supply
    {% if is_incremental() %}
    UNION ALL
    SELECT * FROM existing_supply
    {% endif %}
),
address_contract_pairs AS (
    SELECT DISTINCT
        address,
        contract_address
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
        {{ source('crosschain_gold', 'dim_dates') }}
    WHERE
        date_day < SYSDATE() :: DATE
        AND date_day >= (SELECT MIN(block_date) FROM all_supply)
),
date_address_contract_spine AS (
    SELECT
        d.date_day AS block_date,
        p.address,
        p.contract_address
    FROM
        date_spine d
    CROSS JOIN
        address_contract_pairs p
),
filled_balances AS (
    SELECT
        s.block_date,
        s.address,
        s.contract_address,
        COALESCE(a.balance, 
            LAST_VALUE(a.balance IGNORE NULLS) OVER (
                PARTITION BY s.address, s.contract_address
                ORDER BY s.block_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS balance,
        a.modified_timestamp
    FROM
        date_address_contract_spine s
    LEFT JOIN
        all_supply a
        ON s.block_date = a.block_date
        AND s.address = a.address
        AND s.contract_address = a.contract_address
)
SELECT
    block_date,
    address,
    contract_address,
    balance,
    SYSDATE() AS modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_date','address','contract_address']) }} AS stablecoins_supply_all_address_imputed_id
FROM
    filled_balances
