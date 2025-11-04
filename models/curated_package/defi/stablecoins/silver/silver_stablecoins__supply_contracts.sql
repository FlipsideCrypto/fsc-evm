{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('price__ez_asset_metadata') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ["stablecoins_supply_contracts_id"],
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
dex_pool_list AS (
    SELECT
        DISTINCT pool_address AS address
    FROM
        {{ ref('defi__dim_dex_liquidity_pools') }}
),
lending_pool_list AS (
    SELECT
        DISTINCT protocol_market AS address
    FROM
        {{ ref('defi__ez_lending_deposits') }}
),
contract_list AS (
    SELECT
        DISTINCT address
    FROM
        {{ ref('core__dim_contracts') }}
),
all_balances AS (
    SELECT
        block_date,
        s.address,
        contract_address,
        balance,
        CASE WHEN b.address IS NOT NULL THEN balance ELSE 0 END AS bridge_balance,
        CASE WHEN d.address IS NOT NULL THEN balance ELSE 0 END AS dex_balance,
        CASE WHEN l.address IS NOT NULL THEN balance ELSE 0 END AS lending_pool_balance,
        CASE WHEN c.address IS NOT NULL THEN balance ELSE 0 END AS contracts_balance,
        modified_timestamp
    FROM
        {{ ref('silver_stablecoins__supply_by_address_imputed') }} s
        LEFT JOIN bridge_vault_list b ON b.address = s.address
        LEFT JOIN dex_pool_list d ON d.address = s.address
        LEFT JOIN lending_pool_list l ON l.address = s.address
        LEFT JOIN contract_list c ON c.address = s.address
    WHERE b.address IS NOT NULL OR d.address IS NOT NULL OR l.address IS NOT NULL OR c.address IS NOT NULL

{% if is_incremental() %}
AND
    modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    block_date,
    address,
    contract_address,
    COALESCE(
        bridge_balance,
        0
    ) AS bridge_balance,
    COALESCE(
        dex_balance,
        0
    ) AS dex_balance,
    COALESCE(
        lending_pool_balance,
        0
    ) AS lending_pool_balance,
    COALESCE(
        contracts_balance,
        0
    ) AS contracts_balance,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_date','address','contract_address']) }} AS stablecoins_supply_contracts_id
FROM
    all_balances
