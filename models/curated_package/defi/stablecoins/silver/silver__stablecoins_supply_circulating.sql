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

WITH base_supply AS (

    SELECT
        block_date,
        contract_address,
        SUM(
            s.balance
        ) AS balance,
        MAX(
            s.modified_timestamp
        ) AS modified_timestamp
    FROM
        {{ ref('silver__stablecoins_supply_all_address_imputed') }}
        s

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
mint AS (
    SELECT 
        block_timestamp :: DATE AS block_date,
        contract_address,
        SUM(amount) AS mint_amount
    FROM
        {{ ref('silver__stablecoins_mint_burn') }}
    WHERE event_name IN ('Mint','AddLiquidity','Deposit')
    {% if is_incremental() %}
    AND modified_timestamp > (
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
burn AS (
    SELECT 
        block_timestamp :: DATE AS block_date,
        contract_address,
        SUM(amount) AS burn_amount
    FROM
        {{ ref('silver__stablecoins_mint_burn') }}
    WHERE event_name IN ('Burn','RemoveLiquidity','Withdraw')
    {% if is_incremental() %}
    AND modified_timestamp > (
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
FINAL AS (
    SELECT
        s.block_date,
        s.contract_address,
        s.balance AS total_supply,
        COALESCE(l.balance, 0) AS locked_in_bridges,
        COALESCE(m.mint_amount, 0) AS mint_amount,
        COALESCE(b.burn_amount, 0) AS burn_amount,
        s.balance - COALESCE(
            l.balance,
            0
        ) AS circulating_supply,
        GREATEST(
            s.modified_timestamp,
            COALESCE(
                l.modified_timestamp,
                s.modified_timestamp
            )
        ) AS modified_timestamp
    FROM
        base_supply s
        LEFT JOIN locked_in_bridges l
        ON s.block_date = l.block_date
        AND s.contract_address = l.contract_address
        LEFT JOIN mint m
        ON s.block_date = m.block_date
        AND s.contract_address = m.contract_address
        LEFT JOIN burn b
        ON s.block_date = b.block_date
        AND s.contract_address = b.contract_address
)
SELECT
    block_date,
    contract_address,
    total_supply,
    locked_in_bridges,
    mint_amount,
    burn_amount,
    circulating_supply,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_date','contract_address']) }} AS stablecoins_supply_circulating_id
FROM
    FINAL
