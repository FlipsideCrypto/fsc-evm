{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('price__ez_asset_metadata') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ["stablecoins_supply_contract_balances_id"],
    cluster_by = ['block_date'],
    post_hook = '{{ unverify_stablecoins() }}',
    tags = ['silver','defi','stablecoins','heal','curated']
) }}

WITH contracts AS (
    SELECT
        address,
        contract_type
    FROM
        {{ ref('silver_stablecoins__supply_contract_list') }}
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
balances AS (
    SELECT
        block_date,
        s.address,
        contract_address,
        balance,
        CASE WHEN c0.address IS NOT NULL THEN balance ELSE 0 END AS bridge_balance,
        CASE WHEN c1.address IS NOT NULL THEN balance ELSE 0 END AS dex_balance,
        CASE WHEN c2.address IS NOT NULL THEN balance ELSE 0 END AS lending_pool_balance,
        CASE WHEN c3.address IS NOT NULL THEN balance ELSE 0 END AS contracts_balance,
        CASE WHEN c4.address IS NOT NULL THEN balance ELSE 0 END AS cex_balance,
        s.modified_timestamp
    FROM
        {{ ref('silver_stablecoins__supply_by_address_imputed') }} s
        LEFT JOIN contracts c0 ON c0.address = s.address AND c0.contract_type = 'bridge'
        LEFT JOIN contracts c1 ON c1.address = s.address AND c1.contract_type = 'dex'
        LEFT JOIN contracts c2 ON c2.address = s.address AND c2.contract_type = 'lending'
        LEFT JOIN contracts c3 ON c3.address = s.address AND c3.contract_type = 'all'
        LEFT JOIN contracts c4 ON c4.address = s.address AND c4.contract_type = 'cex'
    WHERE c0.address IS NOT NULL OR c1.address IS NOT NULL OR c2.address IS NOT NULL OR c3.address IS NOT NULL OR c4.address IS NOT NULL

{% if is_incremental() %}
AND
    s.modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    block_date,
    address,
    contract_address,
    COALESCE(
        bridge_balance,
        0
    ) AS bridge_balance,
    COALESCE(
        dex_balance,
        0
    ) AS dex_balance,
    COALESCE(
        lending_pool_balance,
        0
    ) AS lending_pool_balance,
    COALESCE(
        contracts_balance,
        0
    ) AS contracts_balance,
    COALESCE(
        cex_balance,
        0
    ) AS cex_balance,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_date','address','contract_address']) }} AS stablecoins_supply_contract_balances_id
FROM
    balances
