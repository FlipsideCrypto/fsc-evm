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
        user_address,
        contract_address,
        LEAD(event_name) OVER (
            PARTITION BY user_address, contract_address 
            ORDER BY block_timestamp
        ) AS next_event_name,
        LEAD(block_timestamp :: DATE) OVER (
            PARTITION BY user_address, contract_address 
            ORDER BY block_timestamp
        ) AS next_event_date
    FROM 
        {{ ref('silver__stablecoins_address_blacklist') }}
),
blacklist AS (
    SELECT 
        user_address,
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
        block_date,
        contract_address,
        SUM(
            s.balance
        ) AS balance,
        SUM(
            CASE 
                WHEN bl.user_address IS NOT NULL THEN s.balance 
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
        ON s.address = bl.user_address
        AND s.contract_address = bl.contract_address
        AND s.block_date >= bl.start_block_date
        AND (s.block_date < bl.end_block_date OR bl.end_block_date IS NULL)
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
        block_date,
        contract_address
),
locked_in_bridges AS (
    SELECT
        block_date,
        contract_address,
        SUM(
            b.balance
        ) AS balance,
        MAX(
            b.modified_timestamp
        ) AS modified_timestamp
    FROM
        {{ ref('silver__stablecoins_supply_bridge') }}
        b

{% if is_incremental() %}
WHERE
    b.modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
GROUP BY
    block_date,
    contract_address
),
mint_burn AS (
    SELECT
        block_date,
        contract_address,
        event_name,
        SUM(amount) AS mint_burn_amount
    FROM
        {{ ref('silver__stablecoins_mint_burn') }}
    {% if is_incremental() %}
    WHERE modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}
    GROUP BY
        block_date,
        contract_address,
        event_name
),
FINAL AS (
    SELECT
        s.block_date,
        s.contract_address,
        s.balance AS total_supply,
        s.balance_blacklist AS blacklist_supply,
        COALESCE(l.balance, 0) AS locked_in_bridges,
                s.balance - COALESCE(l.balance, 0) - s.balance_blacklist AS circulating_supply,
        CASE WHEN mb.event_name = 'Mint' THEN COALESCE(mb.mint_burn_amount, 0) ELSE 0 END AS mint_amount,
        CASE WHEN mb.event_name = 'Burn' THEN COALESCE(mb.mint_burn_amount, 0) ELSE 0 END AS burn_amount,
        GREATEST(
            s.modified_timestamp,
            COALESCE(l.modified_timestamp, mb.modified_timestamp, s.modified_timestamp)
        ) AS modified_timestamp
    FROM
        base_supply s
        LEFT JOIN locked_in_bridges l
        ON s.block_date = l.block_date
        AND s.contract_address = l.contract_address
        LEFT JOIN mint_burn mb
        ON s.block_date = mb.block_date
        AND s.contract_address = mb.contract_address
)
SELECT
    block_date,
    contract_address,
    total_supply,
    blacklisted_supply,
    locked_in_bridges,
    mint_amount,
    burn_amount,
    circulating_supply,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_date','contract_address']) }} AS stablecoins_supply_circulating_id
FROM
    FINAL
