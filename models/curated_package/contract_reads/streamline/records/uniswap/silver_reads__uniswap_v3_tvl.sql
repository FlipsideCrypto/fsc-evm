{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'uniswap_v3_tvl_id',
    tags = ['silver','contract_reads','curated_daily']
) }}

WITH liquidity_pools AS (
    SELECT
        DISTINCT 
        pool_address,
        token0,
        token1,
        protocol,
        version,
        platform
    FROM {{ ref('silver_dex__poolcreated_evt_v3_pools') }}
    {% if is_incremental() %}
    WHERE modified_timestamp > (
        SELECT MAX(modified_timestamp)
        FROM {{ this }}
    )
    {% endif %}
),

lp_balances AS (
    SELECT
        token0 AS contract_address,
        pool_address AS address,
        token0,
        token1,
        protocol,
        version,
        platform
    FROM liquidity_pools
    UNION
    SELECT
        token1 AS contract_address,
        pool_address AS address,
        token0,
        token1,
        protocol,
        version,
        platform
    FROM liquidity_pools
)

SELECT
    contract_address,
    address,
    'balanceOf' AS function_name,
    '0x70a08231' AS function_sig,
    CONCAT(
        '0x70a08231',
        LPAD(SUBSTR(address, 3), 64, '0')
    ) AS input,
    OBJECT_CONSTRUCT(
        'token0', token0,
        'token1', token1
    ) AS metadata,
    protocol,
    version,
    platform,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address','address','input','platform']
    ) }} AS uniswap_v3_tvl_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM lp_balances
