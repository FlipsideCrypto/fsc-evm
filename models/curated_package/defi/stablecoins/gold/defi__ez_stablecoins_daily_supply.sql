{# Get variables #}
{% set vars = return_vars() %}
{# Log configuration details #}
{{ log_model_details() }}
-- depends_on: {{ ref('price__ez_asset_metadata') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "ez_stablecoins_daily_supply_id",
    cluster_by = ['block_date'],
    post_hook = '{{ unverify_stablecoins() }}',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STABLECOINS',
    }} },
    tags = ['gold','defi','stablecoins','heal','curated']
) }}

WITH mints AS (

    SELECT
        block_timestamp :: DATE AS block_date,
        token_address AS contract_address,
        SUM(amount) AS amount_minted
    FROM
        {{ ref('silver__stablecoins_mint_burn') }}
    WHERE
        tx_succeeded
        AND event_name = 'Mint'

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    ALL
),
burns AS (
    SELECT
        block_timestamp :: DATE AS block_date,
        token_address AS contract_address,
        SUM(amount) AS amount_burnt
    FROM
        {{ ref('silver__stablecoins_mint_burn') }}
    WHERE
        tx_succeeded
        AND event_name = 'Burn'

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    ALL
),
stablecoins_locked_in_bridges AS (
    SELECT
        block_date,
        contract_address,
        symbol,
        SUM(balances) AS amount_locked_in_bridges
    FROM
        {{ ref('silver__stablecoins_locked_in_bridges') }}

{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
GROUP BY
    ALL
),
combined AS (
    SELECT
        block_date,
        contract_address,
        symbol,
        amount_locked_in_bridges,
        COALESCE(
            amount_minted,
            0
        ) AS amount_minted,
        COALESCE(
            amount_burnt,
            0
        ) AS amount_burnt,
    FROM
        stablecoins_locked_in_bridges
        LEFT JOIN mints USING (
            block_date,
            contract_address
        )
        LEFT JOIN burns USING (
            block_date,
            contract_address
        )
),
FINAL AS (
    SELECT
        block_date,
        contract_address,
        symbol,
        amount_locked_in_bridges,
        amount_minted,
        amount_burnt,
        SUM(amount_minted) over (
            PARTITION BY contract_address
            ORDER BY
                block_date ASC
        ) AS cumulative_minted,
        SUM(amount_burnt) over (
            PARTITION BY contract_address
            ORDER BY
                block_date ASC
        ) AS cumulative_burnt,
        cumulative_minted - cumulative_burnt AS total_supply
    FROM
        combined
)
SELECT
    block_date,
    contract_address,
    symbol,
    amount_locked_in_bridges,
    amount_minted,
    amount_burnt,
    cumulative_minted,
    cumulative_burnt,
    total_supply,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_date','contract_address']) }} AS ez_stablecoins_daily_supply_id
FROM
    FINAL
