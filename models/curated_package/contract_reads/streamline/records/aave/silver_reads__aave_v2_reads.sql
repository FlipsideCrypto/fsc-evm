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
        atoken_address AS contract_address,
        underlying_address,
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
    atoken_address AS contract_address,
    underlying_address,
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
{% endif %}
)
SELECT
    contract_address,
    NULL AS address,
    'totalSupply' AS function_name,
    '0x18160ddd' AS function_sig,
    RPAD(
        function_sig,
        64,
        '0'
    ) AS input,
    OBJECT_CONSTRUCT(
        'underlying_address',
        underlying_address
    ) :: variant AS metadata,
    protocol,
    version,
    platform,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address','input','platform']
    ) }} AS aave_v2_reads_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_tokens
