{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['test_silver','decoded_logs','recent_test','phase_2']
) }}

SELECT
    *
FROM
    {{ ref('silver__decoded_logs') }}
WHERE
    block_number > (
        SELECT
            block_number
        FROM
            {{ ref('_block_lookback') }}
    )