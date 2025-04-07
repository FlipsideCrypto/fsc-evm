{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['silver','test_silver','decoded_logs','full_test']
) }}

SELECT
    *
FROM
    {{ ref('silver__decoded_logs') }}