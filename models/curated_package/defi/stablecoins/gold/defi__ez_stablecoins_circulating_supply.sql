{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('price__ez_asset_metadata') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    post_hook = '{{ unverify_stablecoins() }}',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STABLECOINS',
    } } },
    tags = ['gold','defi','stablecoins','heal','curated']
) }}


WITH blacklist AS (

    SELECT
        token_address,
        user_address,
        event_name
    FROM
        {{ ref('silver__stablecoins_address_blacklist') }}
        qualify(ROW_NUMBER() over (PARTITION BY token_address, user_address
    ORDER BY
        block_timestamp DESC)) = 1 --latest blacklist event per token and user
),
supply AS (
    SELECT
        contract_address AS token_address,
        block_date,
        SUM(balance) AS balance,
        SUM(balance_usd) AS balance_usd,
        MAX(modified_timestamp) AS modified_timestamp
    FROM
        {{ ref('balances__ez_balances_erc20_daily') }}
    WHERE
        CONCAT(
            contract_address,
            '-',
            address
        ) NOT IN (
            SELECT
                CONCAT(
                    token_address,
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
GROUP BY contract_address, block_date
)
SELECT
    token_address,
    block_date,
    balance,
    balance_usd,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['token_address', 'block_date']) }} AS ez_stablecoins_circulating_supply_id,
FROM
    supply
