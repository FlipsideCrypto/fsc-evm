{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['test_silver','abis','daily_test','phase_2']
) }}

SELECT
    *
FROM
    {{ ref('silver__verified_abis') }}
WHERE
    _inserted_timestamp > DATEADD(DAY, -5, SYSDATE())
    AND LOWER(abi_source) = LOWER('{{ vars.DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME }}')
