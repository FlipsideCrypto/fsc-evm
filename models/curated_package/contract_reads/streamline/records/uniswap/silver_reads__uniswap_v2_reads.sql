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
dex_pools AS (
    SELECT
        contract_address,
        COUNT(*) AS num_swaps,
        COUNT(
            DISTINCT origin_from_address
        ) AS num_traders
    FROM
        {{ ref('silver_dex__swap_evt_v2_swaps') }}
    {% if is_incremental() %}
    WHERE
        contract_address NOT IN (SELECT contract_address FROM {{ this }})
    {% endif %}
    GROUP BY
        1
    HAVING
        num_swaps >= 500
        AND num_traders >= 500
),
liquidity_pools AS (
    SELECT
        DISTINCT 
        pool_address AS contract_address,
        token0,
        token1,
        protocol,
        version,
        platform,
        CASE 
            WHEN pool_address IN (SELECT contract_address FROM dex_pools) THEN FALSE
            ELSE TRUE
        END AS verified_check_enabled --prevents dex activity driven pools from being deleted via post_hook heal
    FROM {{ ref('silver_dex__paircreated_evt_v2_pools') }}
    WHERE (
        pool_address IN (SELECT contract_address FROM dex_pools)
    )
    OR
    (
    token0 IN (SELECT token_address FROM verified_contracts)
    AND token1 IN (SELECT token_address FROM verified_contracts)
    {% if is_incremental() %}
    AND (
        modified_timestamp > (SELECT MAX(modified_timestamp) FROM {{ this }})
        OR pool_address NOT IN (SELECT contract_address FROM {{ this }})
        -- pull in pools with newly verified tokens
    )
    {% endif %}
    )
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
        'verified_check_enabled', verified_check_enabled::STRING
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
