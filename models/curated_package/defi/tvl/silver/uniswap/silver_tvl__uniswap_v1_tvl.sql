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
        token1 AS contract_address,
        --every pool is token-ETH only, no token-token pairs so TVL = 2x ETH value (pools are always 50/50 and 0x000 e.g. token1, represents native asset)
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
)
SELECT
    block_number,
    block_date,
    contract_address,
    address,
    balance_hex AS amount_hex,
    balance_raw AS amount_raw,
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
