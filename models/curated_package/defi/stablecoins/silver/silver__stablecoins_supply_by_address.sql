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
    tags = ['silver','defi','stablecoins','heal','curated']
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
blacklist AS (
    SELECT
        contract_address,
        user_address,
        event_name
    FROM
        {{ ref('silver__stablecoins_address_blacklist') }}
        qualify(ROW_NUMBER() over (PARTITION BY contract_address, user_address
    ORDER BY
        block_timestamp DESC)) = 1 --latest blacklist event per token and user
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
newly_verified_circ_supply AS (
    SELECT
        block_date,
        address,
        contract_address,
        balance,
        modified_timestamp
    FROM
        {{ ref('balances__ez_balances_erc20_daily') }}
        INNER JOIN newly_verified_stablecoins USING (contract_address)
    WHERE
        CONCAT(
            contract_address,
            '-',
            address
        ) NOT IN (
            SELECT
                CONCAT(
                    contract_address,
                    '-',
                    user_address
                )
            FROM
                blacklist
            WHERE
                event_name = 'AddedBlacklist'
        ) --exclude blacklisted addresses
),
{% endif %}

circ_supply AS (
    SELECT
        block_date,
        address,
        contract_address,
        balance,
        modified_timestamp
    FROM
        {{ ref('balances__ez_balances_erc20_daily') }}
        INNER JOIN verified_stablecoins USING (contract_address)
    WHERE
        CONCAT(
            contract_address,
            '-',
            address
        ) NOT IN (
            SELECT
                CONCAT(
                    contract_address,
                    '-',
                    user_address
                )
            FROM
                blacklist
            WHERE
                event_name = 'AddedBlacklist'
        ) --exclude blacklisted addresses

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
all_circ_supply AS (
    SELECT
        *
    FROM
        circ_supply

{% if is_incremental() and var(
    'HEAL_MODEL',
    false
) %}
UNION
SELECT
    *
FROM
    newly_verified_circ_supply
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
    all_circ_supply
