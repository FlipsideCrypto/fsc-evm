{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['test_gold','decoded_logs','recent_test','phase_3']
) }}

SELECT
    *
FROM
    {{ ref('core__ez_decoded_event_logs') }}
WHERE
    block_number > (
        SELECT
            block_number
        FROM
            {{ ref('_block_lookback') }}
    )