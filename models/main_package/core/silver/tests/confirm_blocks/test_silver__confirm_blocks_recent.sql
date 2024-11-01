{{ config (
    materialized = "view",
    tags = ['recent_test_confirm_blocks']
) }}

SELECT
    *
FROM
    {{ ref('silver__confirm_blocks') }}
WHERE
    modified_timestamp > DATEADD('hour',-12,sysdate()) and 
    partition_key > (
        SELECT
            round(block_number, -3) AS block_number
        FROM
            {{ ref('_block_lookback') }}
    )