{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['test_gold','abis','full_test','phase_2']
) }}

SELECT
    *
FROM
    {{ ref('core__dim_contract_abis') }}