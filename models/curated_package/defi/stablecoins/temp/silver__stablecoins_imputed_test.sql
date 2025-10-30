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
    UNION
    SELECT
        '0xe7c60e30c135f132c18bef795c044e93922a6dea' AS address
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
-- Get the min date by contract and address for new modified timestamp records, including historical replays
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
-- Get all balances by contract and address for new modified timestamp records, including historical replays
incremental_supply AS (
    SELECT
        s.block_date,
        s.address,
        s.contract_address,
        balance,
        modified_timestamp,
        FALSE AS is_imputed
    FROM
        {{ ref('silver__stablecoins_supply_by_address') }}
        s
        INNER JOIN min_base_supply m
        ON s.address = m.address
        AND s.contract_address = m.contract_address
        AND s.block_date >= m.min_base_supply_date
),
-- get a list of distinct address and CA. faster to do count
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
-- get the latest entry for address x token that is not in incremental supply
existing_supply AS (
    SELECT
        block_date,
        address,
        contract_address,
        balance,
        modified_timestamp,
        FALSE AS is_imputed
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
    -- Create a date spine for all dates between the minimum balance date and the current date - 1 day,
    -- Balances are recorded using the last block from the previous day
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

{% if is_incremental() %}
A.is_imputed,
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
