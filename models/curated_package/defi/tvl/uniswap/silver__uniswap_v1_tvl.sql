{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'uniswap_v1_tvl_id',
    tags = ['silver','defi','tvl','curated_daily']
) }}

WITH balances AS (

    SELECT
        b.block_number,
        b.block_date,
        b.address AS contract_address,
        balance_hex,
        balance_raw,
        p.protocol,
        p.version,
        p.platform
    FROM
        {{ ref('balances__ez_balances_native_daily') }}
        b
        LEFT JOIN {{ ref('silver_dex__uniswap_v1_pools') }}
        p
        ON b.address = p.pool_address
    WHERE
        p.address IS NOT NULL
        AND balance_raw IS NOT NULL

{% if is_incremental() %}
AND b.modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_date,
    contract_address,
    NULL AS address,
    balance_hex,
    balance_raw,
    protocol,
    version,
    platform,
    {{ dbt_utils.generate_surrogate_key(
        ['block_date','contract_address','platform']
    ) }} AS uniswap_v1_tvl_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    balances
