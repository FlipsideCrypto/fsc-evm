{# Get variables #}
{% set vars = return_vars() %}
{# Log configuration details #}
{{ log_model_details() }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'aave_v3_reads_id',
    tags = ['silver','contract_reads','curated_daily']
) }}

WITH all_tokens AS (

    SELECT
        atoken_address AS contract_address,
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
        protocol = 'aave'
        AND version = 'v3'

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
    NULL AS address,
    'totalSupply' AS function_name,
    '0x18160ddd' AS function_sig,
    RPAD(
        function_sig,
        64,
        '0'
    ) AS input,
    NULL AS metadata,
    protocol,
    version,
    platform,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address','input','platform']
    ) }} AS aave_v3_reads_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_tokens
