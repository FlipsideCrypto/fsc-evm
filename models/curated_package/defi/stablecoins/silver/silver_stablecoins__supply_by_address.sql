{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('price__ez_asset_metadata') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ["stablecoins_supply_by_address_id"],
    cluster_by = ['block_date'],
    post_hook = '{{ unverify_stablecoins() }}',
    tags = ['silver','defi','stablecoins','heal','curated_daily']
) }}

WITH verified_stablecoins AS (

    SELECT
        contract_address
    FROM
        {{ ref('defi__dim_stablecoins') }}
    WHERE
        is_verified
        AND contract_address IS NOT NULL
),

{% if is_incremental() and var(
    'HEAL_MODEL',
    false
) %}
newly_verified_stablecoins AS (
    SELECT
        contract_address
    FROM
        {{ ref('defi__dim_stablecoins') }}
    WHERE
        IFNULL(
            is_verified_modified_timestamp,
            '1970-01-01' :: TIMESTAMP
        ) > DATEADD(
            'day',
            -8,
            (
                SELECT
                    MAX(modified_timestamp) :: DATE
                FROM
                    {{ this }}
            )
        )
),
newly_verified_supply AS (
    SELECT
        block_date,
        address,
        contract_address,
        balance,
        modified_timestamp
    FROM
        {{ ref('balances__ez_balances_erc20_daily') }}
        INNER JOIN newly_verified_stablecoins USING (contract_address)
),
{% endif %}

supply AS (
    SELECT
        block_date,
        address,
        contract_address,
        balance,
        modified_timestamp
    FROM
        {{ ref('balances__ez_balances_erc20_daily') }}
        INNER JOIN verified_stablecoins USING (contract_address)

{% if is_incremental() %}
WHERE modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
all_supply AS (
    SELECT
        *
    FROM
        supply

{% if is_incremental() and var(
    'HEAL_MODEL',
    false
) %}
UNION
SELECT
    *
FROM
    newly_verified_supply
{% endif %}
)
SELECT
    block_date,
    address,
    contract_address,
    balance,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_date','address','contract_address']) }} AS stablecoins_supply_by_address_id,
FROM
    all_supply
