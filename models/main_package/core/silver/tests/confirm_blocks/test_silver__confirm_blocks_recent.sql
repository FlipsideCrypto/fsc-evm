{{ config (
    materialized = "view",
    tags = ['recent_test_confirm_blocks']
) }}

SELECT
    *
FROM
    {{ ref('silver__confirm_blocks') }}
WHERE
    block_number > (
        SELECT
            block_number
        FROM
            {{ ref('_block_lookback') }}
    )