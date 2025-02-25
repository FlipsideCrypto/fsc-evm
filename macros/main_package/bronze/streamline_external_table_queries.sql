{% macro streamline_external_table_query(
        source_name,
        partition_function,
        uses_receipts_by_hash
    ) %}

    WITH meta AS (
        SELECT
            job_created_time AS _inserted_timestamp,
            file_name,
            {{ partition_function }} AS partition_key
        FROM
            TABLE(
                information_schema.external_table_file_registration_history(
                    start_time => DATEADD('day', -3, CURRENT_TIMESTAMP()),
                    table_name => '{{ source( "bronze_streamline", source_name) }}')
                ) A
            )
        SELECT
            s.*,
            b.file_name,
            b._inserted_timestamp,
            COALESCE(
                s.value :"BLOCK_NUMBER" :: STRING,
                s.metadata :request :"data" :id :: STRING,
                PARSE_JSON(
                    s.metadata :request :"data"
                ) :id :: STRING
            ) :: INT AS block_number
        {% if uses_receipts_by_hash %},
            s.value :"TX_HASH" :: STRING AS tx_hash
        {% endif %}
        FROM
            {{ source(
                "bronze_streamline",
                source_name 
            ) }}
            s
            JOIN meta b
            ON b.file_name = metadata$filename
            AND b.partition_key = s.partition_key
        WHERE
            b.partition_key = s.partition_key
            AND DATA :error IS NULL
            AND DATA IS NOT NULL
{% endmacro %}

{% macro streamline_external_table_query_fr(
        source_name,
        partition_function,
        uses_receipts_by_hash
    ) %}

    
    WITH meta AS (
        SELECT
            registered_on AS _inserted_timestamp,
            file_name,
            {{ partition_function }} AS partition_key
        FROM
            TABLE(
                information_schema.external_table_files(
                    table_name => '{{ source( "bronze_streamline", source_name) }}'
                )
            ) A
    )
SELECT
    s.*,
    b.file_name,
    b._inserted_timestamp,
    COALESCE(
        s.value :"BLOCK_NUMBER" :: STRING,
        s.value :"block_number" :: STRING,
        s.metadata :request :"data" :id :: STRING,
        PARSE_JSON(
            s.metadata :request :"data"
        ) :id :: STRING
    ) :: INT AS block_number
{% if uses_receipts_by_hash %},
    s.value :"TX_HASH" :: STRING AS tx_hash
{% endif %}
FROM
    {{ source(
        "bronze_streamline",
        source_name
    ) }}
    s
    JOIN meta b
    ON b.file_name = metadata$filename
    AND b.partition_key = s.partition_key
WHERE
    b.partition_key = s.partition_key
    AND DATA :error IS NULL
    AND DATA IS NOT NULL
{% endmacro %}
