{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'aerodrome_v1_reads_id',
    tags = ['silver','contract_reads']
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
    FROM {{ ref('silver_dex__aerodrome_pools') }}
    WHERE token0 IN (SELECT token_address FROM verified_contracts)
    AND token1 IN (SELECT token_address FROM verified_contracts)
    {% if is_incremental() %}
    AND modified_timestamp > (
        SELECT MAX(modified_timestamp)
        FROM {{ this }}
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
        'token1', token1
    ) :: VARIANT AS metadata,
    protocol,
    version,
    platform,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address','input','platform']
    ) }} AS aerodrome_v1_reads_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM liquidity_pools
