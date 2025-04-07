{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['gold','test_gold','decoded_logs','full_test']
) }}

SELECT
    *
FROM
    {{ ref('core__ez_decoded_event_logs') }}