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
high_value_pools AS (
    SELECT
        DISTINCT pool_address
    FROM
        {{ ref('defi__ez_dex_liquidity_pool_actions') }}
    WHERE
        event_name IN (
            'Mint',
            'AddLiquidity',
            'Deposit'
        )
        AND amount_usd IS NOT NULL
        AND amount_usd > 0
        AND amount_usd < 1e12 -- filter bad pricing
        AND platform IN (
            SELECT
                DISTINCT platform
            FROM
                {{ ref('silver_dex__paircreated_evt_v2_pools') }}
        )

{% if is_incremental() %}
AND pool_address NOT IN (
    SELECT
        contract_address
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    pool_address
HAVING
    SUM(amount_usd) >= 20000 --update depending on chain, potentially use a var
),
liquidity_pools AS (
    SELECT
        DISTINCT pool_address AS contract_address,
        token0,
        token1,
        protocol,
        version,
        platform,
        -- Track qualification path for unverify logic
        -- If both tokens are verified, subject to unverify. Otherwise protected (high-value pool with unverified tokens)
        CASE
            WHEN token0 IN (SELECT token_address FROM verified_contracts)
                 AND token1 IN (SELECT token_address FROM verified_contracts)
            THEN 'true'
            ELSE 'false'
        END AS verified_check_enabled
    FROM
        {{ ref('silver_dex__paircreated_evt_v2_pools') }}
    WHERE
        (
            -- High value pools
            pool_address IN (
                SELECT
                    pool_address
                FROM
                    high_value_pools
            )
            OR (
                -- Both tokens verified
                token0 IN (
                    SELECT
                        token_address
                    FROM
                        verified_contracts
                )
                AND token1 IN (
                    SELECT
                        token_address
                    FROM
                        verified_contracts
                )
            )
        )

{% if is_incremental() %}
AND (
    modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
    OR pool_address NOT IN (
        SELECT
            contract_address
        FROM
            {{ this }}
    )
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
        'token0',
        token0,
        'token1',
        token1,
        'verified_check_enabled',
        verified_check_enabled
    ) :: variant AS metadata,
    protocol,
    version,
    platform,
    {{ dbt_utils.generate_surrogate_key(['contract_address', 'input', 'platform']) }} AS uniswap_v2_reads_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    liquidity_pools
