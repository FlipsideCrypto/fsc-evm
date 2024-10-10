{# {% set uses_receipts_by_hash = var('GLOBAL_USES_RECEIPTS_BY_HASH', false) %}
{% if uses_receipts_by_hash %}
{% set source_name = var('RECEIPTS_BY_HASH_SOURCE_NAME', 'RECEIPTS_BY_HASH') %}
{% set model_type = '' %}

{%- set default_vars = set_default_variables_bronze(source_name, model_type) -%}

{{ log_bronze_details(
    source_name = source_name,
    model_type = model_type,
    partition_function = default_vars['partition_function'],
    partition_join_key = default_vars['partition_join_key'],
    block_number = default_vars['block_number'],
    uses_receipts_by_hash = default_vars['uses_receipts_by_hash']
) }}

{{ config (
    materialized = 'view',
    tags = ['streamline_core_complete', 'bronze_external']
) }}

    WITH meta AS (

        SELECT
            job_created_time AS _inserted_timestamp,
            file_name,
            {{ default_vars['partition_function'] }} AS partition_key
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

        {% if default_vars['block_number'] %},
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
{% endif %} #}