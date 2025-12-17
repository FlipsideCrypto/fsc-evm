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
        p.token0,
        p.token1,
        b.address,
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
        p.pool_address IS NOT NULL
        AND balance_raw IS NOT NULL

{% if is_incremental() %}
AND b.modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),

expanded AS (
    -- Row for token0 as contract_address
    SELECT
        block_number,
        block_date,
        token0 AS contract_address,
        token1 AS token_address,
        address,
        balance_hex,
        balance_raw,
        protocol,
        version,
        platform
    FROM
        balances

    UNION ALL

    -- Row for token1 as contract_address
    SELECT
        block_number,
        block_date,
        token1 AS contract_address,
        token1 AS token_address,
        address,
        balance_hex,
        balance_raw,
        protocol,
        version,
        platform
    FROM
        balances
)

SELECT
    block_number,
    block_date,
    contract_address,
    address,
    token_address,
    balance_hex AS amount_hex,
    balance_raw AS amount_raw,
    protocol,
    version,
    platform,
    {{ dbt_utils.generate_surrogate_key(
        ['block_date','contract_address','token_address','platform']
    ) }} AS uniswap_v1_tvl_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    expanded qualify(ROW_NUMBER() over(PARTITION BY uniswap_v1_tvl_id
ORDER BY
    block_number DESC)) = 1
