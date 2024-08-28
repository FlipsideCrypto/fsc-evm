{% macro number_sequence(
    max_num=1000000000
) %}
SELECT
    ROW_NUMBER() over (
        ORDER BY
            SEQ4()
    ) - 1 :: INT AS _id
FROM
    TABLE(GENERATOR(rowcount => max_num))
{% endmacro %}

{% macro block_sequence() %}
SELECT
    _id AS block_number,
    utils.udf_int_to_hex(_id) AS block_number_hex
FROM
    {{ ref(
        'silver__number_sequence'
    ) }}
WHERE
    _id <= (
        SELECT
            COALESCE(
                block_number,
                0
            )
        FROM
            {{ ref("streamline__get_chainhead") }}
    )
ORDER BY
    _id ASC
{% endmacro %}

{% macro block_ranges() %}
SELECT
    block_number,
    CASE
        WHEN RIGHT(
            block_number,
            1
        ) = 0 THEN block_number
    END AS block_number_10,
    CASE
        WHEN RIGHT(
            block_number,
            2
        ) IN (
            00,
            25,
            50,
            75
        ) THEN block_number
    END AS block_number_25,
    CASE
        WHEN RIGHT(
            block_number,
            2
        ) IN (
            00,
            50
        ) THEN block_number
    END AS block_number_50,
    CASE
        WHEN RIGHT(
            block_number,
            2
        ) IN (00) THEN block_number
    END AS block_number_100,
    CASE
        WHEN RIGHT(
            block_number,
            3
        ) IN (000) THEN block_number
    END AS block_number_1000,
    CASE
        WHEN RIGHT(
            block_number,
            4
        ) IN (0000) THEN block_number
    END AS block_number_10000,
    block_timestamp,
    TO_TIMESTAMP_NTZ(_inserted_timestamp) AS _inserted_timestamp
FROM
    {{ ref("silver__blocks") }}
{% endmacro %}
