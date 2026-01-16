{# Get variables #}
{% set vars = return_vars() %}
{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'polymarket_v1_reads_id',
    tags = ['silver','contract_reads']
) }}

WITH contracts AS (
    SELECT
        '0x2791bca1f2de4661ed88a30c99a7a9449aa84174' AS contract_address, --USDC
        '0x4d97dcd97ec945f40cf65f87097ace5ea0476045' AS address --Conditional Tokens
    UNION ALL
    SELECT
        '0x2791bca1f2de4661ed88a30c99a7a9449aa84174' AS contract_address, --USDC
        '0x3a3bd7bb9528e159577f7c2e685cc81a765002e2' AS address --Collateral Tokens
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
    NULL :: VARIANT AS metadata,
    'polymarket' AS protocol,
    'v1' AS version,
    CONCAT(protocol, '-', version) AS platform,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address','address','input','platform']
    ) }} AS polymarket_v1_reads_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    contracts
{% if is_incremental() %}
WHERE CONCAT(contract_address, '-', address) NOT IN (
    SELECT
        CONCAT(contract_address, '-', address)
    FROM
        {{ this }}
)
{% endif %}
