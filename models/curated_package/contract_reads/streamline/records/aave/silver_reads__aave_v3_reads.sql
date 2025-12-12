{# Get variables #}
{% set vars = return_vars() %}
{# Log configuration details #}
{{ log_model_details() }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'aave_v3_reads_id',
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
        version = 'v3'

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
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
    ) }} AS aave_v3_reads_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_tokens
