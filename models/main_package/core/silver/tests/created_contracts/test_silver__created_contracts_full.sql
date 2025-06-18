{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['test_silver','core','full_test','phase_2']
) }}

SELECT
    *
FROM
    {{ ref('silver__created_contracts') }}
