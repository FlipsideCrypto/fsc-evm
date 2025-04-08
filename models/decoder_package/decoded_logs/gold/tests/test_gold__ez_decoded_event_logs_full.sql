{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['test_gold','decoded_logs','full_test','phase_2']
) }}

SELECT
    *
FROM
    {{ ref('core__ez_decoded_event_logs') }}