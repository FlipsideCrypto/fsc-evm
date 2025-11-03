{# Get variables #}
{% set vars = return_vars() %}
{# Log configuration details #}
{{ log_model_details() }}
-- depends_on: {{ ref('price__ez_asset_metadata') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ["stablecoins_supply_circulating_id"],
    cluster_by = ['block_date'],
    post_hook = '{{ unverify_stablecoins() }}',
    tags = ['silver','defi','stablecoins','heal','curated']
) }}

WITH blacklist_ordered_evt AS (

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
        {{ ref('silver__stablecoins_address_blacklist') }}
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
base_supply AS (
    SELECT
        s.block_date,
        s.contract_address,
        SUM(
            s.balance
        ) AS balance,
        SUM(
            CASE
                WHEN bl.blacklist_address IS NOT NULL THEN s.balance
                ELSE 0
            END
        ) AS balance_blacklist,
        MAX(
            s.modified_timestamp
        ) AS modified_timestamp
    FROM
        {{ ref('silver__stablecoins_supply_by_address_imputed') }}
        s
        LEFT JOIN blacklist bl
        ON s.address = bl.blacklist_address
        AND s.contract_address = bl.contract_address
        AND s.block_date >= bl.start_block_date
        AND (
            s.block_date < bl.end_block_date
            OR bl.end_block_date IS NULL
        )

{% if is_incremental() %}
WHERE
    s.modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
GROUP BY
    s.block_date,
    s.contract_address
),
locked_in_contracts_dates AS (
    SELECT
        DISTINCT block_date
    FROM
        {{ ref('silver__stablecoins_supply_contracts') }}

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
locked_in_contracts AS (
    SELECT
        block_date,
        contract_address,
        SUM(bridge_balance) AS bridge_balance,
        SUM(dex_balance) AS dex_balance,
        SUM(lending_pool_balance) AS lending_pool_balance,
        SUM(contracts_balance) AS contracts_balance,
        MAX(modified_timestamp) AS modified_timestamp
    FROM
        {{ ref('silver__stablecoins_supply_contracts') }}
        INNER JOIN locked_in_contracts_dates USING (block_date)
    GROUP BY
        block_date,
        contract_address
),
mint_burn_dates AS (
    SELECT
        DISTINCT block_timestamp :: DATE AS block_date
    FROM
        {{ ref('silver__stablecoins_mint_burn') }}

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
        {{ ref('silver__stablecoins_mint_burn') }}
        m
        INNER JOIN mint_burn_dates d
        ON m.block_timestamp :: DATE = d.block_date
    GROUP BY
        block_date,
        contract_address
),
transfers_dates AS (
    SELECT
        DISTINCT block_date
    FROM
        {{ ref('silver__stablecoins_transfer') }}

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
transfers AS (
    SELECT
        block_date,
        contract_address,
        SUM(amount) AS transfer_volume,
        MAX(modified_timestamp) AS modified_timestamp
    FROM
        {{ ref('silver__stablecoins_transfer') }}
        INNER JOIN transfers_dates USING (block_date)
    GROUP BY
        block_date,
        contract_address
),
FINAL AS (
    SELECT
        s.block_date,
        s.contract_address,
        s.balance AS total_supply,
        s.balance_blacklist AS blacklist_supply,
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
            mb.mint_amount,
            0
        ) AS mint_amount,
        COALESCE(
            mb.burn_amount,
            0
        ) AS burn_amount,
        s.balance - COALESCE(
            l.balance,
            0
        ) - s.balance_blacklist AS circulating_supply,
        transfer_volume,
        GREATEST(
            s.modified_timestamp,
            COALESCE(
                l.modified_timestamp,
                mb.modified_timestamp,
                s.modified_timestamp,
                t.modified_timestamp
            )
        ) AS modified_timestamp
    FROM
        base_supply s
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
    stablecoin_label,
    total_supply,
    blacklist_supply,
    bridge_balance,
    dex_balance,
    lending_pool_balance,
    contracts_balance,
    mint_amount,
    burn_amount,
    circulating_supply,
    transfer_volume,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_date','contract_address']) }} AS stablecoins_supply_circulating_id
FROM
    FINAL
    LEFT JOIN {{ ref('defi__dim_stablecoins') }} USING (contract_address)
