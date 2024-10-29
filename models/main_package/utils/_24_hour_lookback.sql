{{ config(
    materialized = 'ephemeral'
) }}

    WITH max_time AS (
        SELECT
            MAX(block_timestamp) AS max_timestamp
        FROM
            {{ ref("core__fact_blocks") }}
    )
SELECT
    COALESCE(MIN(block_number), 0) AS block_number
FROM
    {{ ref("core__fact_blocks") }}
    JOIN max_time
    ON block_timestamp BETWEEN DATEADD(
        'hour',
        -25,
        max_timestamp
    )
    AND DATEADD(
        'hour',
        -24,
        max_timestamp
        )