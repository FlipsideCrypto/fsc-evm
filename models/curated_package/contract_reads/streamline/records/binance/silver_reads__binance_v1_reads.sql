{# Get variables #}
{% set vars = return_vars() %}
{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'binance_v1_reads_id',
    tags = ['silver','contract_reads']
) }}

WITH contracts AS (
    SELECT
        '0xa2e3356610840701bdf5611a53974510ae27e2e1' AS contract_address --Wrapped Binance Beacon ETH (wBETH)
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
    NULL :: VARIANT AS metadata,
    'binance' AS protocol,
    'v1' AS version,
    CONCAT(protocol, '-', version) AS platform,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address','input','platform']
    ) }} AS binance_v1_reads_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    contracts
