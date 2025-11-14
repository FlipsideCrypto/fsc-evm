{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('price__ez_asset_metadata') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ["stablecoins_supply_contracts_list_id"],
    tags = ['silver','defi','stablecoins','heal','curated_daily']
) }}

WITH bridge_vault_list AS (

    SELECT
        DISTINCT bridge_address AS address,
        'bridge' AS contract_type
    FROM
        {{ ref('defi__ez_bridge_activity') }}

{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
UNION
SELECT
    vault_address AS address,
    'bridge' AS contract_type
FROM
    {{ ref('silver_stablecoins__bridge_vault_seed') }}
WHERE
    chain = '{{ vars.GLOBAL_PROJECT_NAME }}'

{% if is_incremental() %}
AND address NOT IN (
    SELECT
        address
    FROM
        {{ this }}
    WHERE
        contract_type = 'bridge'
)
{% endif %}
),
dex_pool_list AS (
    SELECT
        DISTINCT pool_address AS address,
        'dex' AS contract_type
    FROM
        {{ ref('defi__dim_dex_liquidity_pools') }}

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
lending_pool_list AS (
    SELECT
        DISTINCT protocol_market AS address,
        'lending' AS contract_type
    FROM
        {{ ref('defi__ez_lending_deposits') }}

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
contract_list AS (
    SELECT
        DISTINCT address,
        'all' AS contract_type
    FROM
        {{ ref('core__dim_contracts') }}

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
cex_list AS (
    SELECT
        DISTINCT address,
        'cex' AS contract_type
    FROM
        {{ ref('core__dim_labels') }}
    WHERE
        label_type = 'cex'

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
all_contracts AS (
    SELECT
        *
    FROM
        bridge_vault_list
    UNION ALL
    SELECT
        *
    FROM
        dex_pool_list
    UNION ALL
    SELECT
        *
    FROM
        lending_pool_list
    UNION ALL
    SELECT
        *
    FROM
        contract_list
    UNION ALL
    SELECT
        *
    FROM
        cex_list
)
SELECT
    address,
    contract_type,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['address','contract_type']) }} AS stablecoins_supply_contracts_list_id
FROM
    all_contracts
