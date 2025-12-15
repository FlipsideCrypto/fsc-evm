{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'lido_reads_id',
    tags = ['silver','contract_reads']
) }}

WITH pooled_assets AS (
SELECT
    '0xae7ab96520de3a18e5e111b5eaab095312d7fe84' AS contract_address,
    'getTotalPooledEther' AS function_name,
    '0x37cfdaca' AS function_sig,
    RPAD(
        function_sig,
        64,
        '0'
    ) AS input
UNION ALL
SELECT
    '0x9ee91f9f426fa633d227f7a9b000e28b9dfd8599' AS contract_address,
    'getTotalPooledMatic' AS function_name,
    '0xe00222a0' AS function_sig,
    RPAD(
        function_sig,
        64,
        '0'
    ) AS input
)

SELECT
    contract_address,
    NULL AS address,
    function_name,
    function_sig,
    input,
    NULL :: VARIANT AS metadata,
    'lido' AS protocol,
    'v1' AS version,
    CONCAT(protocol, '-', version) AS platform,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address','input','platform']
    ) }} AS lido_reads_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM pooled_assets