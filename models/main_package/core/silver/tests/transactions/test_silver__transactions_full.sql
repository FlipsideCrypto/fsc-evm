{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['silver','test_silver','core','full_test']
) }}

SELECT
    *
FROM
    {{ ref('silver__transactions') }}