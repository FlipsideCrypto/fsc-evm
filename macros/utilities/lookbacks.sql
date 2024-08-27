{% macro block_lookback_24_hour() %}
    WITH max_time AS (
        SELECT
            MAX(block_timestamp) AS max_timestamp
        FROM
            {{ ref("silver__blocks") }}
    )
SELECT
    MIN(block_number) AS block_number
FROM
    {{ ref("silver__blocks") }}
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
{% endmacro %}

{% macro block_lookback_72_hour() %}
SELECT
    MIN(block_number) AS block_number
FROM
    {{ ref("silver__blocks") }}
WHERE
    block_timestamp >= DATEADD('hour', -72, TRUNCATE(SYSDATE(), 'HOUR'))
    AND block_timestamp < DATEADD('hour', -71, TRUNCATE(SYSDATE(), 'HOUR'))
{% endmacro %}

{% macro max_block_by_date() %}
    WITH base AS (
        SELECT
            block_timestamp :: DATE AS block_date,
            MAX(block_number) block_number
        FROM
            {{ ref("silver__blocks") }}
        GROUP BY
            block_timestamp :: DATE
    )
SELECT
    block_date,
    block_number
FROM
    base
WHERE
    block_date <> (
        SELECT
            MAX(block_date)
        FROM
            base
    )
{% endmacro %}

{% macro max_block_by_hour() %}
    WITH base AS (
        SELECT
            DATE_TRUNC(
                'hour',
                block_timestamp
            ) AS block_hour,
            MAX(block_number) block_number
        FROM
            {{ ref("silver__blocks") }}
        WHERE
            block_timestamp > DATEADD(
                'day',
                -5,
                CURRENT_DATE
            )
        GROUP BY
            1
    )
SELECT
    block_hour,
    block_number
FROM
    base
WHERE
    block_hour <> (
        SELECT
            MAX(
                block_hour
            )
        FROM
            base
    )
{% endmacro %}
