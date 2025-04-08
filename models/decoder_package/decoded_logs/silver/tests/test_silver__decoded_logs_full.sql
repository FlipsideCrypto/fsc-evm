{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['test_silver','decoded_logs','full_test','phase_2']
) }}

SELECT
    *
FROM
    {{ ref('silver__decoded_logs') }}