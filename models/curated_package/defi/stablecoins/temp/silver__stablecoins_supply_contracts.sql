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
all_contracts AS (
    SELECT
        DISTINCT address
    FROM
        {{ ref('core__dim_contracts') }}
),
bridges AS (
    SELECT
        block_date,
        address,
        contract_address,
        balance AS bridge_balance,
        modified_timestamp
    FROM
        {{ ref('silver__stablecoins_supply_by_address_imputed') }}
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
dexes AS (
    SELECT
        block_date,
        address,
        contract_address,
        balance AS dex_balance,
        modified_timestamp
    FROM
        {{ ref('silver__stablecoins_supply_by_address_imputed') }}
        INNER JOIN dex_pool_list USING (address)

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
lending_markets AS (
    SELECT
        block_date,
        address,
        contract_address,
        balance AS lending_pool_balance,
        modified_timestamp
    FROM
        {{ ref('silver__stablecoins_supply_by_address_imputed') }}
        INNER JOIN lending_pool_list USING (address)

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
all_contracts AS (
    SELECT
        block_date,
        address,
        contract_address,
        balance AS contracts_balance,
        modified_timestamp
    FROM
        {{ ref('silver__stablecoins_supply_by_address_imputed') }}
        INNER JOIN all_contracts USING (address)

{% if is_incremental() %}
WHERE
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
    all_contracts
    LEFT JOIN bridges USING (
        block_date,
        address,
        contract_address
    )
    LEFT JOIN dexes USING (
        block_date,
        address,
        contract_address
    )
    LEFT JOIN lending_markets USING (
        block_date,
        address,
        contract_address
    )
