{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['gold','test_gold','abis','daily_test']
) }}

SELECT
    *
FROM
    {{ ref('core__dim_contract_abis') }}
WHERE
    inserted_timestamp > DATEADD(DAY, -5, SYSDATE())