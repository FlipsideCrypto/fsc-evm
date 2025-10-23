{# Get variables #}
{% set vars = return_vars() %}
{# Log configuration details #}
{{ log_model_details() }}
-- depends_on: {{ ref('price__ez_asset_metadata') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ["stablecoins_locked_in_bridges_id"],
    cluster_by = ['block_date'],
    tags = ['gold','defi','stablecoins','heal','curated']
) }}
-- post_hook = '{{ unverify_stablecoins() }}',
WITH verified_stablecoins AS (

    SELECT
        token_address AS contract_address
    FROM
        {{ ref('defi__dim_stablecoins') }}
    WHERE
        is_verified
        AND token_address IS NOT NULL
),
bridge_vault_list AS (
    SELECT
        DISTINCT bridge_address
    FROM
        {{ ref('defi__ez_bridge_activity') }}
    UNION
    SELECT
        vault_address AS bridge_address
    FROM
        {{ ref('silver_stablecoins__bridge_vault_seed') }}
    WHERE
        chain = '{{ vars.GLOBAL_PROJECT_NAME }}'
),
raw_balances AS (
    SELECT
        block_date AS days,
        address,
        contract_address,
        symbol,
        balance,
        balance_precise,
        balance_raw
    FROM
        {{ ref('balances__ez_balances_erc20_daily') }}
        INNER JOIN verified_stablecoins USING (contract_address)
    WHERE
        address IN (
            SELECT
                bridge_address
            FROM
                bridge_vault_list
        )
),
dates AS (
    SELECT
        date_day
    FROM
        {{ source(
            'crosschain_gold',
            'dim_dates'
        ) }}
    WHERE
        date_day BETWEEN '2025-06-10'
        AND CURRENT_DATE()
),
address_token_list AS (
    SELECT
        address,
        contract_address,
        COUNT(1)
    FROM
        raw_balances
    GROUP BY
        ALL
),
full_list AS (
    SELECT
        date_day AS days,
        address,
        contract_address
    FROM
        dates
        CROSS JOIN address_token_list
),
balances_dates AS (
    SELECT
        f.days AS block_date,
        f.address,
        f.contract_address,
        IFF(balance IS NULL, LAG(balance) ignore nulls over (PARTITION BY address, contract_address
    ORDER BY
        f.days ASC), balance) AS balances
    FROM
        full_list f
        LEFT JOIN raw_balances b USING (
            days,
            address,
            contract_address
        )
)
SELECT
    block_date,
    address,
    contract_address,
    balances,
    {{ dbt_utils.generate_surrogate_key(['block_date','address','contract_address']) }} AS stablecoins_locked_in_bridges_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    balances_dates
