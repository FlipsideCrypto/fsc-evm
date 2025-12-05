{# Get variables #}
{% set vars = return_vars() %}
{# Log configuration details #}
{{ log_model_details() }}
-- depends_on: {{ ref('price__ez_asset_metadata') }}
-- depends_on: {{ ref('defi__dim_stablecoins') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ["stablecoins_supply_complete_id"],
    cluster_by = ['block_date'],
    post_hook = '{{ unverify_stablecoins() }}',
    tags = ['silver','defi','stablecoins','heal','curated_daily']
) }}

WITH

{% if is_incremental() %}
max_ts AS (
    SELECT MAX(modified_timestamp) AS max_modified_timestamp
    FROM {{ this }}
),
incremental_dates AS (
    -- Get all distinct dates that have been updated in any source table
    SELECT DISTINCT block_date
    FROM {{ ref('silver__stablecoin_reads') }}
    WHERE modified_timestamp > (SELECT max_modified_timestamp FROM max_ts)
    UNION
    SELECT DISTINCT block_date
    FROM {{ ref('silver_stablecoins__supply_by_address_imputed') }}
    WHERE modified_timestamp > (SELECT max_modified_timestamp FROM max_ts)
    UNION
    SELECT DISTINCT block_date
    FROM {{ ref('silver_stablecoins__supply_contracts') }}
    WHERE modified_timestamp > (SELECT max_modified_timestamp FROM max_ts)
    UNION
    SELECT DISTINCT block_timestamp::DATE AS block_date
    FROM {{ ref('silver_stablecoins__mint_burn') }}
    WHERE modified_timestamp > (SELECT max_modified_timestamp FROM max_ts)
    UNION
    SELECT DISTINCT block_date
    FROM {{ ref('silver_stablecoins__transfers') }}
    WHERE modified_timestamp > (SELECT max_modified_timestamp FROM max_ts)
),
{% endif %}

total_supply AS (
    SELECT
        block_date,
        contract_address,
        amount AS total_supply,
        metadata :symbol :: STRING AS symbol,
        metadata :name :: STRING AS name,
        metadata :label :: STRING AS label,
        metadata :decimals :: INTEGER AS decimals
    FROM
        {{ ref('silver__stablecoin_reads') }}

{% if is_incremental() %}
WHERE
    block_date IN (
        SELECT
            block_date
        FROM
            incremental_dates
    )
{% endif %}
),
blacklist_ordered_evt AS (
    SELECT
        block_timestamp :: DATE AS block_date,
        blacklist_address,
        contract_address,
        event_name,
        LEAD(event_name) over (
            PARTITION BY blacklist_address,
            contract_address
            ORDER BY
                block_timestamp
        ) AS next_event_name,
        LEAD(
            block_timestamp :: DATE
        ) over (
            PARTITION BY blacklist_address,
            contract_address
            ORDER BY
                block_timestamp
        ) AS next_event_date
    FROM
        {{ ref('silver_stablecoins__address_blacklist') }}
),
blacklist AS (
    SELECT
        blacklist_address,
        contract_address,
        block_date AS start_block_date,
        CASE
            WHEN next_event_name = 'RemovedBlacklist' THEN next_event_date
            ELSE NULL
        END AS end_block_date
    FROM
        blacklist_ordered_evt
    WHERE
        event_name = 'AddedBlacklist'
),
blacklist_supply AS (
    SELECT
        s.block_date,
        s.contract_address,
        SUM(
            s.balance
        ) AS balance_blacklist,
        MAX(
            s.modified_timestamp
        ) AS modified_timestamp
    FROM
        {{ ref('silver_stablecoins__supply_by_address_imputed') }}
        s
        LEFT JOIN blacklist bl
        ON s.address = bl.blacklist_address
        AND s.contract_address = bl.contract_address
        AND s.block_date >= bl.start_block_date
        AND (
            s.block_date < bl.end_block_date
            OR bl.end_block_date IS NULL
        )
    WHERE
        bl.blacklist_address IS NOT NULL

{% if is_incremental() %}
AND s.block_date IN (
    SELECT
        block_date
    FROM
        incremental_dates
)
{% endif %}
GROUP BY
    s.block_date,
    s.contract_address
),
locked_in_contracts AS (
    SELECT
        block_date,
        contract_address,
        SUM(bridge_balance) AS bridge_balance,
        SUM(dex_balance) AS dex_balance,
        SUM(lending_pool_balance) AS lending_pool_balance,
        SUM(cex_balance) AS cex_balance,
        SUM(contracts_balance) AS contracts_balance,
        MAX(modified_timestamp) AS modified_timestamp
    FROM
        {{ ref('silver_stablecoins__supply_contracts') }}

{% if is_incremental() %}
WHERE
    block_date IN (
        SELECT
            block_date
        FROM
            incremental_dates
    )
{% endif %}
GROUP BY
    block_date,
    contract_address
),
mint_burn AS (
    SELECT
        block_timestamp :: DATE AS block_date,
        contract_address,
        SUM(
            CASE
                WHEN event_name = 'Mint' THEN amount
                ELSE 0
            END
        ) AS mint_amount,
        SUM(
            CASE
                WHEN event_name = 'Burn' THEN amount
                ELSE 0
            END
        ) AS burn_amount,
        MAX(modified_timestamp) AS modified_timestamp
    FROM
        {{ ref('silver_stablecoins__mint_burn') }}
        m

{% if is_incremental() %}
WHERE
    block_timestamp :: DATE IN (
        SELECT
            block_date
        FROM
            incremental_dates
    )
{% endif %}
GROUP BY
    block_timestamp :: DATE,
    contract_address
),
transfers AS (
    SELECT
        block_date,
        contract_address,
        SUM(amount) AS transfer_volume,
        MAX(modified_timestamp) AS modified_timestamp
    FROM
        {{ ref('silver_stablecoins__transfers') }}

{% if is_incremental() %}
WHERE
    block_date IN (
        SELECT
            block_date
        FROM
            incremental_dates
    )
{% endif %}
GROUP BY
    block_date,
    contract_address
),
holders AS (
    SELECT
        block_date,
        contract_address,
        COUNT(DISTINCT address) AS num_holders,
        MAX(modified_timestamp) AS modified_timestamp
    FROM
        {{ ref('silver_stablecoins__supply_by_address_imputed') }}
WHERE balance > 0
{% if is_incremental() %}
AND block_date IN (
    SELECT
        block_date
    FROM
        incremental_dates
)
{% endif %}
GROUP BY
    block_date,
    contract_address
),
all_supply AS (
    SELECT
        s.block_date,
        s.contract_address,
        s.total_supply,
        s.symbol,
        s.name,
        s.label,
        s.decimals,
        COALESCE(
            b.balance_blacklist,
            0
        ) AS blacklist_supply,
        COALESCE(
            l.bridge_balance,
            0
        ) AS bridge_balance,
        COALESCE(
            l.dex_balance,
            0
        ) AS dex_balance,
        COALESCE(
            l.lending_pool_balance,
            0
        ) AS lending_pool_balance,
        COALESCE(
            l.contracts_balance,
            0
        ) AS contracts_balance,
        COALESCE(
            l.cex_balance,
            0
        ) AS cex_balance,
        COALESCE(
            mb.mint_amount,
            0
        ) AS mint_amount,
        COALESCE(
            mb.burn_amount,
            0
        ) AS burn_amount,
        COALESCE(
            transfer_volume,
            0
        ) AS transfer_volume,
        COALESCE(
            num_holders,
            0
        ) AS num_holders
    FROM
        total_supply s
        LEFT JOIN blacklist_supply b
        ON s.block_date = b.block_date
        AND s.contract_address = b.contract_address
        LEFT JOIN locked_in_contracts l
        ON s.block_date = l.block_date
        AND s.contract_address = l.contract_address
        LEFT JOIN mint_burn mb
        ON s.block_date = mb.block_date
        AND s.contract_address = mb.contract_address
        LEFT JOIN transfers t
        ON s.block_date = t.block_date
        AND s.contract_address = t.contract_address
        LEFT JOIN holders h
        ON s.block_date = h.block_date
        AND s.contract_address = h.contract_address
),

{% if is_incremental() and var(
    'HEAL_MODEL'
) %}
heal_model AS (
    SELECT
        t.block_date,
        t.contract_address,
        d.symbol AS symbol_heal,
        d.name AS name_heal,
        d.label AS label_heal,
        d.decimals AS decimals_heal,
        t.total_supply,
        t.amount_blacklisted,
        t.amount_in_cex,
        t.amount_in_bridges,
        t.amount_in_dex_liquidity_pools,
        t.amount_in_lending_pools,
        t.amount_in_contracts,
        t.amount_minted,
        t.amount_burned,
        t.amount_transferred,
        t.total_holders
    FROM
        {{ this }}
        t
        INNER JOIN {{ ref('defi__dim_stablecoins') }}
        d
        ON t.contract_address = d.contract_address
    WHERE
        t.symbol IS NULL
        OR t.name IS NULL
        OR t.decimals IS NULL
),
{% endif %}

FINAL AS (
    SELECT
        block_date,
        contract_address,
        symbol,
        NAME,
        label,
        decimals,
        total_supply,
        blacklist_supply AS amount_blacklisted,
        cex_balance AS amount_in_cex,
        bridge_balance AS amount_in_bridges,
        dex_balance AS amount_in_dex_liquidity_pools,
        lending_pool_balance AS amount_in_lending_pools,
        contracts_balance AS amount_in_contracts,
        mint_amount AS amount_minted,
        burn_amount AS amount_burned,
        transfer_volume AS amount_transferred,
        num_holders AS total_holders
    FROM
        all_supply

{% if is_incremental() and var(
    'HEAL_MODEL'
) %}
UNION ALL
SELECT
    block_date,
    contract_address,
    symbol_heal AS symbol,
    name_heal AS NAME,
    label_heal AS label,
    decimals_heal AS decimals,
    total_supply,
    amount_blacklisted,
    amount_in_cex,
    amount_in_bridges,
    amount_in_dex_liquidity_pools,
    amount_in_lending_pools,
    amount_in_contracts,
    amount_minted,
    amount_burned,
    amount_transferred,
    total_holders
FROM
    heal_model
{% endif %}
)
SELECT
    block_date,
    contract_address,
    symbol,
    NAME,
    label,
    decimals,
    total_supply,
    total_holders,
    amount_blacklisted,
    amount_in_cex,
    amount_in_bridges,
    amount_in_dex_liquidity_pools,
    amount_in_lending_pools,
    amount_in_contracts,
    amount_minted,
    amount_burned,
    amount_transferred,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_date','contract_address']) }} AS stablecoins_supply_complete_id
FROM
    FINAL

{% if is_incremental() and var(
    'HEAL_MODEL'
) %}
qualify(ROW_NUMBER() over (PARTITION BY stablecoins_supply_complete_id
ORDER BY
    modified_timestamp DESC)) = 1
{% endif %}
