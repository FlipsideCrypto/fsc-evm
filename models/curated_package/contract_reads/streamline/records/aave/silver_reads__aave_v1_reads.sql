{# Get variables #}
{% set vars = return_vars() %}
{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('silver_lending__aave_ethereum_tokens') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'aave_v1_reads_id',
    tags = ['silver','contract_reads']
) }}

WITH all_tokens AS (

    SELECT
        underlying_address AS contract_address,
        protocol,
        version,
        CONCAT(
            protocol,
            '-',
            version
        ) AS platform
    FROM
        {{ ref('silver_lending__aave_tokens') }}
    WHERE
        version = 'v1'

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}

{% if vars.GLOBAL_PROJECT_NAME == 'ethereum' %}
UNION
SELECT
    underlying_address AS contract_address,
    protocol,
    version,
    CONCAT(
        protocol,
        '-',
        version
    ) AS platform
FROM
    {{ ref('silver_lending__aave_ethereum_tokens') }}
    --relevant for ethereum only
WHERE
    version = 'v1'

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
{% endif %}
),
lending_pools AS (
    SELECT
        contract_address,
        CASE
            WHEN '{{ vars.GLOBAL_PROJECT_NAME }}' = 'ethereum' THEN '0x3dfd23a6c5e8bbcfc9581d2e864a68feb6a076d3'
        END AS address,
        --Aave: LendingPoolCore
        protocol,
        version,
        platform
    FROM
        all_tokens
    UNION ALL
    SELECT
        contract_address,
        CASE
            WHEN '{{ vars.GLOBAL_PROJECT_NAME }}' = 'ethereum' THEN '0x1012cff81a1582ddd0616517efb97d02c5c17e25'
        END AS address,
        --Uniswap: LendingPoolCore in Aave v1 holds Uniswap v1 LP tokens as collateral
        protocol,
        version,
        platform
    FROM
        all_tokens
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
    NULL :: VARIANT AS metadata,
    protocol,
    version,
    platform,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address','address','input','platform']
    ) }} AS aave_v1_reads_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    lending_pools
