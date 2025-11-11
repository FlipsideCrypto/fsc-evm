{# Get variables #}
{% set vars = return_vars() %}
{# Log configuration details #}
{{ log_model_details() }}
-- depends_on: {{ ref('price__ez_asset_metadata') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ["stablecoins_supply_complete_id"],
    cluster_by = ['block_date'],
    post_hook = '{{ unverify_stablecoins() }}',
    tags = ['silver','defi','stablecoins','heal','curated']
) }}

WITH total_supply AS (

    SELECT
        block_date,
        contract_address,
        total_supply
    FROM
        {{ ref('silver__stablecoin_reads') }}

{% if is_incremental() %}
WHERE
    block_date IN (
        SELECT
            DISTINCT block_date
        FROM
            {{ ref('silver__stablecoin_reads') }}
        WHERE
            modified_timestamp > (
                SELECT
                    MAX(modified_timestamp)
                FROM
                    {{ this }}
            )
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
AND
    block_date IN (
        SELECT
            DISTINCT block_date
        FROM
            {{ ref('silver_stablecoins__supply_by_address_imputed') }}
        WHERE
            modified_timestamp > (
                SELECT
                    MAX(modified_timestamp)
                FROM
                    {{ this }}
            )
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
            DISTINCT block_date
        FROM
            {{ ref('silver_stablecoins__supply_contracts') }}
        WHERE
            modified_timestamp > (
                SELECT
                    MAX(modified_timestamp)
                FROM
                    {{ this }}
            )
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
            DISTINCT block_timestamp :: DATE
        FROM
            {{ ref('silver_stablecoins__mint_burn') }}
        WHERE
            modified_timestamp > (
                SELECT
                    MAX(modified_timestamp)
                FROM
                    {{ this }}
            )
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
            DISTINCT block_date
        FROM
            {{ ref('silver_stablecoins__transfers') }}
        WHERE
            modified_timestamp > (
                SELECT
                    MAX(modified_timestamp)
                FROM
                    {{ this }}
            )
    )
{% endif %}
GROUP BY
    block_date,
    contract_address
),
FINAL AS (
    SELECT
        s.block_date,
        s.contract_address,
        s.total_supply,
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
        ) AS transfer_volume
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
)
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
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_date','contract_address']) }} AS stablecoins_supply_complete_id
FROM
    FINAL
    LEFT JOIN {{ ref('defi__dim_stablecoins') }} USING (contract_address)
