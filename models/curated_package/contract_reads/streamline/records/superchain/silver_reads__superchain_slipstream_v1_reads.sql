{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'aerodrome_v2_reads_id',
    tags = ['silver','contract_reads']
) }}

WITH blacklisted_tokens AS (
    SELECT LOWER('0xdbfefd2e8460a6ee4955a68582f85708baea60a3') AS token_address -- superOETHb: excluded to avoid double-counting with Origin Protocol TVL (Aerodrome AMO position)
),
liquidity_pools AS (
    SELECT
        DISTINCT
        pool_address,
        token0_address AS token0,
        token1_address AS token1,
        protocol,
        version,
        platform
    FROM {{ ref('silver_dex__superchain_slipstream_pools') }}
    WHERE token0 NOT IN (SELECT token_address FROM blacklisted_tokens)
    AND token1 NOT IN (SELECT token_address FROM blacklisted_tokens)
    {% if is_incremental() %}
    AND modified_timestamp > (
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
    ) :: VARIANT AS metadata,
    protocol,
    version,
    platform,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address','input','platform']
    ) }} AS aerodrome_v2_reads_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM lp_balances
