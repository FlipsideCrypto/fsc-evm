{% set uses_receipts_by_hash = var('USES_RECEIPTS_BY_HASH', false) %}

{% if uses_receipts_by_hash %}

{% set source_name = var('RECEIPTS_BY_HASH_SOURCE_NAME', 'RECEIPTS_BY_HASH') %}

{% set model_type = '' %}

{# Default dynamic variables begin #}

{% set partition_function = var(source_name ~ model_type ~ '_PARTITION_FUNCTION', 
 "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER)") 
%}

{% set partition_join_key = var(source_name ~ model_type ~ '_PARTITION_JOIN_KEY', 'partition_key') %}
{% set block_number = var(source_name ~ model_type ~ '_BLOCK_NUMBER', True) %}

{# Default variables end #}

{# Log configuration details if in dev or during execution #}
{%- if execute and not target.name.startswith('prod') -%}

    {{ log("=== Current Variable Settings ===", info=True) }}
    {{ log(source_name ~ model_type ~ '_PARTITION_FUNCTION: ' ~ partition_function, info=True) }}
    {{ log(source_name ~ model_type ~ '_PARTITION_JOIN_KEY: ' ~ partition_join_key, info=True) }}
    {{ log(source_name ~ model_type ~ '_BLOCK_NUMBER: ' ~ block_number, info=True) }}
    {{ log("Uses Receipts by Hash: " ~ uses_receipts_by_hash, info=True) }}

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
    tags = ['streamline_core_complete']
) }}

    WITH meta AS (

        SELECT
            job_created_time AS _inserted_timestamp,
            file_name,
            {{ partition_function }} AS partition_key
        FROM
            TABLE(
                information_schema.external_table_file_registration_history(
                    start_time => DATEADD('day', -3, CURRENT_TIMESTAMP()),
                    table_name => '{{ source( "bronze_streamline", source_name.lower()) }}')
                ) A
            )
        SELECT
            s.*,
            b.file_name,
            b._inserted_timestamp

        {% if block_number %},
            COALESCE(
                s.value :"BLOCK_NUMBER" :: STRING,
                s.metadata :request :"data" :id :: STRING,
                PARSE_JSON(
                    s.metadata :request :"data"
                ) :id :: STRING
            ) :: INT AS block_number
        {% endif %}
            , s.value :"TX_HASH" :: STRING AS tx_hash
        FROM
            {{ source(
                "bronze_streamline",
                source_name.lower()
            ) }}
            s
            JOIN meta b
            ON b.file_name = metadata$filename
            AND b.partition_key = s.partition_key
        WHERE
            b.partition_key = s.partition_key
            AND DATA :error IS NULL
            AND DATA IS NOT NULL
{% endif %}