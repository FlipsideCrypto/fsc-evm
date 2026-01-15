{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'uniswap_v2_reads_id',
    post_hook = '{{ unverify_contract_reads() }}',
    tags = ['silver','contract_reads','heal']
) }}

WITH verified_contracts AS (
    SELECT
        DISTINCT token_address
    FROM
        {{ ref('price__ez_asset_metadata') }}
    WHERE
        is_verified
        AND token_address IS NOT NULL
),
liquidity_pools AS (
    SELECT
        DISTINCT 
        pool_address AS contract_address,
        token0,
        token1,
        protocol,
        version,
        platform
    FROM {{ ref('silver_dex__paircreated_evt_v2_pools') }}
    WHERE token0 IN (SELECT token_address FROM verified_contracts)
    AND token1 IN (SELECT token_address FROM verified_contracts)
    {% if is_incremental() %}
    AND (
        modified_timestamp > (SELECT MAX(modified_timestamp) FROM {{ this }})
        OR pool_address NOT IN (SELECT contract_address FROM {{ this }})
        -- pull in pools with newly verified tokens
    )
    {% endif %}
)
SELECT
    contract_address,
    NULL AS address,
    'getReserves' AS function_name,
    '0x0902f1ac' AS function_sig,
    RPAD(
        function_sig,
        64,
        '0'
    ) AS input,
    OBJECT_CONSTRUCT(
        'token0', token0,
        'token1', token1,
        'verified_check_enabled','true'
    ) :: VARIANT AS metadata,
    protocol,
    version,
    platform,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address','input','platform']
    ) }} AS uniswap_v2_reads_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM liquidity_pools