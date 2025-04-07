{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['silver','test_silver','abis','full_test']
) }}

SELECT
    *
FROM
    {{ ref('silver__abis') }}
