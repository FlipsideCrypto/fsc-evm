{% macro streamline_external_table_query(
        source_name,
        source_version='',
        partition_function="CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER)",
        error_code=false,
        balances=false,
        block_number=true,
        tx_hash=false,
        contract_address=false
    ) %}

    {% if source_version != '' %}
        {% set source_version = '_' ~ source_version.lower() %}
    {% endif %}

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

            {% if balances %}, --for balances
            r.block_timestamp :: TIMESTAMP AS block_timestamp
        {% endif %}

        {% if block_number %}, --for streamline 2.0+
            COALESCE(
                s.value :"BLOCK_NUMBER" :: STRING,
                s.metadata :request :"data" :id :: STRING,
                PARSE_JSON(
                    s.metadata :request :"data"
                ) :id :: STRING
            ) :: INT AS block_number
        {% endif %}

        {% if contract_address %}, --for contract_abis
            COALESCE(
                VALUE :"CONTRACT_ADDRESS",
                VALUE :"contract_address"
            ) :: STRING AS contract_address
        {% endif %}

        {% if tx_hash %}, --for receipts_by_hash
            s.value :"TX_HASH" :: STRING AS tx_hash
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
            {% if error_code %}
            AND DATA :error :code IS NULL
            {% else %}
            AND DATA :error IS NULL
            {% endif %}
            AND DATA IS NOT NULL
{% endmacro %}

{% macro streamline_external_table_query_fr(
        source_name,
        source_version='',
        partition_function="CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER)",
        partition_join_key='partition_key',
        error_code=false,
        balances=false,
        block_number=true,
        tx_hash=false,
        contract_address=false
    ) %}

    {% if source_version != '' %}
        {% set source_version = '_' ~ source_version.lower() %}
    {% endif %}
    
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

    {% if balances %}, --for balances
    r.block_timestamp :: TIMESTAMP AS block_timestamp
{% endif %}

{% if block_number %}, --for streamline 2.0+
    COALESCE(
        s.value :"BLOCK_NUMBER" :: STRING,
        s.value :"block_number" :: STRING,
        s.metadata :request :"data" :id :: STRING,
        PARSE_JSON(
            s.metadata :request :"data"
        ) :id :: STRING
    ) :: INT AS block_number
{% endif %}

{% if contract_address %}, --for contract_abis
    COALESCE(
        VALUE :"CONTRACT_ADDRESS",
        VALUE :"contract_address"
    ) :: STRING AS contract_address
{% endif %}

{% if tx_hash %}, --for receipts_by_hash
    s.value :"TX_HASH" :: STRING AS tx_hash
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
    {% if error_code %}
    AND DATA :error :code IS NULL
    {% else %}
    AND DATA :error IS NULL
    {% endif %}
    AND DATA IS NOT NULL
{% endmacro %}
