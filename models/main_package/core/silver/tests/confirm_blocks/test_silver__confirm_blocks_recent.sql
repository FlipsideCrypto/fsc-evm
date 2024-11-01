{{ config (
    materialized = "view",
    tags = ['recent_test_confirm_blocks']
) }}

SELECT
    *
FROM
    {{ ref('silver__confirm_blocks') }}
WHERE
    modified_timestamp > DATEADD(
        'hour' (
            SELECT
                IFF(
                    DATEDIFF('hour', MIN(modified_timestamp), SYSDATE()) <= (
                        24 * 6
                    ),
                    -12,
                    -24 * 5
                )
            FROM
                {{ ref('silver__confirm_blocks') }}
        ),
        SYSDATE()
    )
    AND partition_key > (
        SELECT
            ROUND(
                block_number,
                -3
            ) AS block_number
        FROM
            {{ ref('_block_lookback') }}
    )
