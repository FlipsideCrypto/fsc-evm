{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'uniswap_v4_reads_id',
    tags = ['silver','contract_reads','curated_daily']
) }}

WITH liquidity_pools AS (

    SELECT
        token0,
        token1,
        pool_address AS factory_address,
        hook_address,
        protocol,
        version,
        platform
    FROM
        {{ ref('silver_dex__uniswap_v4_pools') }}

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
all_balances AS (
    SELECT
        token0 AS contract_address,
        factory_address AS address,
        token0,
        token1,
        hook_address,
        'factory' AS address_type,
        protocol,
        version,
        platform
    FROM
        liquidity_pools
    WHERE
        token0 IS NOT NULL
        AND token0 <> '0x0000000000000000000000000000000000000000' -- Represents native asset. balanceOf calls only apply to erc20 tokens. eth_getBalance calls to be handled downstream for null/native assets.
    UNION
    SELECT
        token1 AS contract_address,
        factory_address AS address,
        token0,
        token1,
        hook_address,
        'factory' AS address_type,
        protocol,
        version,
        platform
    FROM
        liquidity_pools
    WHERE
        token1 IS NOT NULL
        AND token1 <> '0x0000000000000000000000000000000000000000'
    UNION
    SELECT
        token0 AS contract_address,
        hook_address AS address,
        token0,
        token1,
        hook_address,
        'hook' AS address_type,
        protocol,
        version,
        platform
    FROM
        liquidity_pools
    WHERE
        hook_address IS NOT NULL
        AND hook_address <> '0x0000000000000000000000000000000000000000'
        AND token0 IS NOT NULL
        AND token0 <> '0x0000000000000000000000000000000000000000'
    UNION
    SELECT
        token1 AS contract_address,
        hook_address AS address,
        token0,
        token1,
        hook_address,
        'hook' AS address_type,
        protocol,
        version,
        platform
    FROM
        liquidity_pools
    WHERE
        hook_address IS NOT NULL
        AND hook_address <> '0x0000000000000000000000000000000000000000'
        AND token1 IS NOT NULL
        AND token1 <> '0x0000000000000000000000000000000000000000'
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
    object_construct_keep_null(
        'token0',
        token0,
        'token1',
        token1,
        'hook_address',
        hook_address,
        'address_type',
        address_type
    ) :: variant AS metadata,
    protocol,
    version,
    platform,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address','input','platform']
    ) }} AS uniswap_v4_reads_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_balances
