{% macro streamline_external_table_query(
        source_name,
        source_version,
        partition_function,
        balances,
        block_number
    ) %}

    {% set source_version = '_' ~ source_version.lower() %}

    WITH meta AS (
        SELECT
            job_created_time AS _inserted_timestamp,
            file_name,
            {{ partition_function }} AS partition_key
        FROM
            TABLE(
                information_schema.external_table_file_registration_history(
                    start_time => DATEADD('day', -3, CURRENT_TIMESTAMP()),
                    table_name => '{{ source( "bronze_streamline", source_name ~ source_version) }}')
                ) A
            )
        SELECT
            s.*,
            b.file_name,
            b._inserted_timestamp

            {% if balances %},
            r.block_timestamp :: TIMESTAMP AS block_timestamp
        {% endif %}

        {% if block_number %},
            COALESCE(
                s.value :"BLOCK_NUMBER" :: STRING,
                s.metadata :request :"data" :id :: STRING,
                PARSE_JSON(
                    s.metadata :request :"data"
                ) :id :: STRING
            ) :: INT AS block_number
        {% endif %}
        FROM
            {{ source(
                "bronze_streamline",
                source_name ~ source_version
            ) }}
            s
            JOIN meta b
            ON b.file_name = metadata$filename
            AND b.partition_key = s.partition_key

            {% if balances %}
            JOIN {{ ref('_block_ranges') }}
            r
            ON r.block_number = COALESCE(
                s.value :"BLOCK_NUMBER" :: INT,
                s.value :"block_number" :: INT
            )
        {% endif %}
        WHERE
            b.partition_key = s.partition_key
            AND DATA :error IS NULL
            AND DATA IS NOT NULL
{% endmacro %}

{% macro streamline_external_table_fr_query(
        source_name,
        source_version,
        partition_function,
        partition_join_key = "partition_key",
        balances = false,
        block_number = true
    ) %}
    WITH meta AS (
        SELECT
            registered_on AS _inserted_timestamp,
            file_name,
            {{ partition_function }} AS partition_key
        FROM
            TABLE(
                information_schema.external_table_files(
                    table_name => '{{ source( "bronze_streamline", source_name ~ source_version) }}'
                )
            ) A
    )
SELECT
    s.*,
    b.file_name,
    b._inserted_timestamp

    {% if balances %},
    r.block_timestamp :: TIMESTAMP AS block_timestamp
{% endif %}

{% if block_number %},
    COALESCE(
        s.value :"BLOCK_NUMBER" :: STRING,
        s.value :"block_number" :: STRING,
        s.metadata :request :"data" :id :: STRING,
        PARSE_JSON(
            s.metadata :request :"data"
        ) :id :: STRING
    ) :: INT AS block_number
{% endif %}
FROM
    {{ source(
        "bronze_streamline",
        source_name ~ source_version
    ) }}
    s
    JOIN meta b
    ON b.file_name = metadata$filename
    AND b.partition_key = s.{{ partition_join_key }}

    {% if balances %}
        JOIN {{ ref('_block_ranges') }}
        r
        ON r.block_number = COALESCE(
            s.value :"BLOCK_NUMBER" :: INT,
            s.value :"block_number" :: INT
        )
    {% endif %}
WHERE
    b.partition_key = s.{{ partition_join_key }}
    AND DATA :error IS NULL
    AND DATA IS NOT NULL
{% endmacro %}

{% macro streamline_external_table_fr_union_query(
        model
    ) %}
SELECT
    partition_key,
    block_number,
    {% if model == 'receipts' or model == 'traces' %}
        array_index,
    {% endif %}

    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__streamline_fr_' ~ model ~ '_v2') }}
UNION ALL
SELECT
    _partition_by_block_id AS partition_key,
    block_number,
    {% if model == 'receipts' or model == 'traces' %}
        VALUE :"array_index" :: INT AS array_index,
    {% endif %}

    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__streamline_fr_' ~ model ~ '_v1') }}
{% endmacro %}

{% macro streamline_external_table_query_decoder(
        model
    ) %}
    WITH meta AS (
        SELECT
            job_created_time AS _inserted_timestamp,
            file_name,
            CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 6), '_', 1) AS INTEGER) AS _partition_by_block_number,
            TO_DATE(
                concat_ws('-', SPLIT_PART(file_name, '/', 3), SPLIT_PART(file_name, '/', 4), SPLIT_PART(file_name, '/', 5))
            ) AS _partition_by_created_date
        FROM
            TABLE(
                information_schema.external_table_file_registration_history(
                    start_time => DATEADD('day', -3, CURRENT_TIMESTAMP()),
                    table_name => '{{ source( "bronze_streamline", model) }}')
                ) A
            )
        SELECT
            block_number,
            id :: STRING AS id,
            DATA,
            metadata,
            b.file_name,
            _inserted_timestamp,
            s._partition_by_block_number AS _partition_by_block_number,
            s._partition_by_created_date AS _partition_by_created_date
        FROM
            {{ source(
                "bronze_streamline",
                model
            ) }}
            s
            JOIN meta b
            ON b.file_name = metadata$filename
            AND b._partition_by_block_number = s._partition_by_block_number
            AND b._partition_by_created_date = s._partition_by_created_date
        WHERE
            b._partition_by_block_number = s._partition_by_block_number
            AND b._partition_by_created_date = s._partition_by_created_date
            AND s._partition_by_created_date >= DATEADD('day', -2, CURRENT_TIMESTAMP())
            AND DATA :error IS NULL
            AND DATA IS NOT NULL
{% endmacro %}

{% macro streamline_external_table_fr_query_decoder(
        model
    ) %}
    WITH meta AS (
        SELECT
            registered_on AS _inserted_timestamp,
            file_name,
            CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 6), '_', 1) AS INTEGER) AS _partition_by_block_number,
            TO_DATE(
                concat_ws('-', SPLIT_PART(file_name, '/', 3), SPLIT_PART(file_name, '/', 4), SPLIT_PART(file_name, '/', 5))
            ) AS _partition_by_created_date
        FROM
            TABLE(
                information_schema.external_table_files(
                    table_name => '{{ source( "bronze_streamline", model) }}'
                )
            ) A
    )
SELECT
    block_number,
    id :: STRING AS id,
    DATA,
    metadata,
    b.file_name,
    _inserted_timestamp,
    s._partition_by_block_number AS _partition_by_block_number,
    s._partition_by_created_date AS _partition_by_created_date
FROM
    {{ source(
        "bronze_streamline",
        model
    ) }}
    s
    JOIN meta b
    ON b.file_name = metadata$filename
    AND b._partition_by_block_number = s._partition_by_block_number
    AND b._partition_by_created_date = s._partition_by_created_date
WHERE
    b._partition_by_block_number = s._partition_by_block_number
    AND b._partition_by_created_date = s._partition_by_created_date
    AND DATA :error IS NULL
    AND DATA IS NOT NULL
{% endmacro %}