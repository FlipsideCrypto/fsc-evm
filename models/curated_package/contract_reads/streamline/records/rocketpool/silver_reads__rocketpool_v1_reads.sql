{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'rocketpool_v1_reads_id',
    tags = ['silver','contract_reads']
) }}

SELECT
    '0xae78736cd615f374d3085123a210448e74fc6393' AS contract_address, --rETH
    NULL AS address,
    'totalSupply' AS function_name,
    '0x18160ddd' AS function_sig,
    RPAD(
        function_sig,
        64,
        '0'
    ) AS input,
    NULL :: VARIANT AS metadata,
    'rocketpool' AS protocol,
    'v1' AS version,
    CONCAT(protocol, '-', version) AS platform,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address','input','platform']
    ) }} AS rocketpool_v1_reads_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
{% if is_incremental() %}
WHERE contract_address NOT IN (
    SELECT
        contract_address
    FROM
        {{ this }}
)
{% endif %}
