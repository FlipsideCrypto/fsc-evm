{% set source_name = var('BLOCKS_FR_SOURCE_NAME', 'BLOCKS')%}

{% set partition_function = 
    var(source_name ~ '_FR_PARTITION_FUNCTION', 
        "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER)") 
%}

{% set partition_join_key = var(source_name ~ '_FR_PARTITION_JOIN_KEY', 'partition_key') %}
{% set balances = var(source_name ~ '_FR_BALANCES', false) %}
{% set block_number = var(source_name ~ '_FR_BLOCK_NUMBER', true) %}

{# Log configuration details if in dev or during execution #}
{%- if execute and not target.name.startswith('prod') -%}

    {{ log("=== Current Variable Settings ===", info=True) }}
    {{ log(source_name ~ '_FR_PARTITION_FUNCTION: ' ~ partition_function, info=True) }}
    {{ log(source_name ~ '_FR_PARTITION_JOIN_KEY: ' ~ partition_join_key, info=True) }}
    {{ log(source_name ~ '_FR_BALANCES: ' ~ balances, info=True) }}
    {{ log(source_name ~ '_FR_BLOCK_NUMBER: ' ~ block_number, info=True) }}

    {{ log("", info=True) }}
    {{ log("=== Source Details ===", info=True) }}
    {{ log("Source: " ~ source('bronze_streamline', source_name.lower()), info=True) }}
    {{ log("", info=True) }}

    {% set config_log = '\n' %}
    {% set config_log = config_log ~ '\n=== DBT Model Config ===\n'%}
    {% set config_log = config_log ~ '\n{{ config (\n' %}
    {% set config_log = config_log ~ '    materialized = "' ~ config.get('materialized') ~ '",\n' %}
    {% set config_log = config_log ~ '    tags = ' ~ config.get('tags') | tojson ~ '\n' %}
    {% set config_log = config_log ~ ') }}\n' %}
    {{ log(config_log, info=True) }}
    {{ log("", info=True) }}

{% endif %}

{{ config (
    materialized = 'view',
    tags = ['test_tag']
) }}

    WITH meta AS (
        SELECT
            registered_on AS _inserted_timestamp,
            file_name,
            {{ partition_function }} AS partition_key
        FROM
            TABLE(
                information_schema.external_table_files(
                    table_name => '{{ source( "bronze_streamline", source_name.lower()) }}')
                ) A
            )
        SELECT
            s.*,
            b.file_name,
            b._inserted_timestamp
        {% if balances %}
            , r.block_timestamp :: TIMESTAMP AS block_timestamp
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
                source_name.lower()
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