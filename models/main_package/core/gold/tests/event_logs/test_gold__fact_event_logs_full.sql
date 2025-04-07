{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['test_gold','core','full_test','phase_2']
) }}

SELECT
    *
FROM
    {{ ref('core__fact_event_logs') }}