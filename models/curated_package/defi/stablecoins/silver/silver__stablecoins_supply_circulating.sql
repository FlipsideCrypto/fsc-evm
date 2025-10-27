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
FINAL AS (
    SELECT
        b.block_date,
        b.contract_address,
        b.balance AS total_supply,
        l.balance AS locked_in_bridges,
        b.balance - COALESCE(
            l.balance,
            0
        ) AS circulating_supply,
        GREATEST(
            b.modified_timestamp,
            COALESCE(
                l.modified_timestamp,
                b.modified_timestamp
            )
        ) AS modified_timestamp
    FROM
        base_supply b
        LEFT JOIN locked_in_bridges l
        ON b.block_date = l.block_date
        AND b.contract_address = l.contract_address
)
SELECT
    block_date,
    contract_address,
    total_supply,
    locked_in_bridges,
    circulating_supply,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_date','contract_address']) }} AS stablecoins_supply_circulating_id
FROM
    FINAL
