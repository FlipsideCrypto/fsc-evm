{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'sky_v1_reads_id',
    tags = ['silver','contract_reads']
) }}

WITH collateral_joins AS (
    SELECT
        token_address AS contract_address,
        join_address AS address,
        protocol,
        version,
        platform
    FROM
        {{ ref('silver_reads__sky_v1_collateral_joins') }}

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
    NULL::VARIANT AS metadata,
    protocol,
    version,
    platform,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address','address','input','platform']
    ) }} AS sky_v1_reads_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    collateral_joins
