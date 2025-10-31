{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('price__ez_asset_metadata') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ["stablecoins_supply_bridge_id"],
    cluster_by = ['block_date'],
    post_hook = '{{ unverify_stablecoins() }}',
    tags = ['silver','defi','stablecoins','heal','curated']
) }}

WITH bridge_vault_list AS (
    SELECT
        DISTINCT bridge_address AS address
    FROM
        {{ ref('defi__ez_bridge_activity') }}
    UNION
    SELECT
        vault_address AS address
    FROM
        {{ ref('silver_stablecoins__bridge_vault_seed') }}
    WHERE
        chain = '{{ vars.GLOBAL_PROJECT_NAME }}'
),
locked_in_bridges AS (
    SELECT
        block_date,
        address,
        contract_address,
        balance,
        modified_timestamp
    FROM
        {{ ref('silver__stablecoins_supply_by_address_imputed') }} s 
        INNER JOIN bridge_vault_list USING (address)
    {% if is_incremental() %}
    WHERE
        modified_timestamp > (
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
    balance,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_date','address','contract_address']) }} AS stablecoins_supply_bridge_id
FROM
    locked_in_bridges