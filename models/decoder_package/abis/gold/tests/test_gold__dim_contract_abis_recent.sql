{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['test_gold','abis','daily_test','phase_2']
) }}

SELECT
    *
FROM
    {{ ref('core__dim_contract_abis') }}
WHERE
    inserted_timestamp > DATEADD(DAY, -5, SYSDATE())