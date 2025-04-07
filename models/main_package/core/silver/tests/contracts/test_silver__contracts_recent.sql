{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['test_silver','core','daily_test','phase_2']
) }}

SELECT
    *
FROM
    {{ ref('silver__contracts') }}
WHERE
    inserted_timestamp > DATEADD(DAY, -5, SYSDATE())
