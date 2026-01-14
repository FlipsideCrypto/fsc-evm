{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'compound_v2_reads_id',
    tags = ['silver','contract_reads']
) }}

WITH all_tokens AS (

    SELECT
        underlying_asset_address AS contract_address,
        token_address AS address,
        protocol,
        version,
        CONCAT(
            protocol,
            '-',
            version
        ) AS platform
    FROM
        {{ ref('silver_lending__comp_v2_asset_details') }}

{% if is_incremental() %}
WHERE modified_timestamp > (
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
        function_sig,
        LPAD(SUBSTR(address, 3), 64, '0')
    ) AS input,
    NULL :: variant AS metadata,
    protocol,
    version,
    platform,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address','address','input','platform']
    ) }} AS compound_v2_reads_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_tokens
