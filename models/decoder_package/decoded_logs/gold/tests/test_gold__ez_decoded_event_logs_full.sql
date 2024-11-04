{{ config (
    materialized = "view",
    tags = ['full_test']
) }}

SELECT
    *
FROM
    {{ ref('core__ez_decoded_event_logs') }}