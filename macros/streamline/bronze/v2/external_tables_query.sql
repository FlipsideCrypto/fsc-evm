{% macro streamline_external_table_query_v2() %}

{# Extract model information from the identifier #}
{%- set identifier_parts = this.identifier.split('__') -%}
{%- if '__' in this.identifier -%}
    {%- set model_parts = identifier_parts[1].split('_') -%}
    {%- if model_parts | length > 1 -%}
        {%- set model_type = model_parts[-1] -%}
        {%- set model = '_'.join(model_parts[:-1]) -%}
    {%- else -%}
        {%- set model_type = 'incremental' -%}
        {%- set model = identifier_parts[1] -%}
    {%- endif -%}
{%- else -%}
    {%- set model_parts = this.identifier.split('_') -%}
    {%- if model_parts | length > 1 -%}
        {%- set model_type = model_parts[-1] -%}
        {%- set model = '_'.join(model_parts[:-1]) -%}
    {%- else -%}
        {%- set model_type = 'incremental' -%}
        {%- set model = this.identifier -%}
    {%- endif -%}
{%- endif -%}

{# Set parameters using project variables #}
{% set partition_function = var((model ~ '_partition_function').upper(), 
    "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER)") %}
{% set balances = var((model ~ '_balances').upper(), false) %}
{% set block_number = var((model ~ '_block_number').upper(), true) %}

{# Log configuration details if in execution mode #}
{% if execute %}
    {{ log("", info=True) }}
    {{ log("=== Model Configuration ===", info=True) }}
    {{ log("Model: " ~ model, info=True) }}
    {{ log("Model Type: " ~ model_type, info=True) }}
    {{ log("Partition Function: " ~ partition_function, info=True) }}
    {{ log("Balances: " ~ balances, info=True) }}
    {{ log("Block Number: " ~ block_number, info=True) }}
    {{ log("Materialization: " ~ config.get('materialized'), info=True) }}
    {{ log("", info=True) }}

    {{ log("=== Source Details ===", info=True) }}
    {{ log("Source: " ~ source('bronze_streamline', model), info=True) }}
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
                    table_name => '{{ source( "bronze_streamline", model) }}')
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
                model
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