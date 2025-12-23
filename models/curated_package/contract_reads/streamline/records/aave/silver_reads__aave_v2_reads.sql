{# Get variables #}
{% set vars = return_vars() %}
{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('silver_lending__aave_ethereum_tokens') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'aave_v2_reads_id',
    tags = ['silver','contract_reads']
) }}

WITH all_tokens AS (

    SELECT
        underlying_address AS contract_address,
        atoken_address AS address,
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
        version = 'v2'

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
    atoken_address AS address,
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
    version IN ('v2','v2.1')

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
UNION
SELECT
    '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9' AS contract_address, --AAVE
    '0x4da27a545c0c5b758a6ba100e3a049001de870f5' AS address, --stkAAVE (Staking)
    'aave' AS protocol,
    'v2' AS version,
    CONCAT(
        protocol,
        '-',
        version
    ) AS platform
{% endif %}
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
    NULL :: variant AS metadata,
    protocol,
    version,
    platform,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address','address','input','platform']
    ) }} AS aave_v2_reads_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_tokens
