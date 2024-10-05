{% set uses_receipts_by_hash = var('USES_RECEIPTS_BY_HASH', false) %}

{% if uses_receipts_by_hash %}

{# Extract model information from the identifier #}
{%- set identifier_parts = this.identifier.split('__') -%}
{%- if '__' in this.identifier -%}
    {%- set model = identifier_parts[1] -%}
{%- else -%}
    {%- set model = this.identifier -%}
{%- endif -%}

{# Dynamically get the trim suffix for this specific model #}
{% set trim_suffix = var((model ~ 'trim_suffix').upper(), '') %}

{# Trim model name logic and extract model_type #}
{%- if trim_suffix and model.endswith(trim_suffix) -%}
    {%- set trimmed_model = model[:model.rfind(trim_suffix)] -%}
    {%- set model_type = trim_suffix[1:] -%}  {# Remove the leading underscore #}
{%- else -%}
    {%- set trimmed_model = model -%}
    {%- set model_type = 'limited_bronze_view' -%}
{%- endif -%}

{# Set parameters using project variables #}
{% set partition_function = var((trimmed_model ~ '_partition_function').upper(), 
    "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER)") %}
{% set balances = var((trimmed_model ~ '_balances').upper(), false) %}
{% set block_number = var((trimmed_model ~ '_block_number').upper(), true) %}


{# Log configuration details if in dev or during execution #}
{%- if execute and not target.name.startswith('prod') -%}

    {{ log("=== Name Output Details ===", info=True) }}

    {{ log("Original Model: " ~ model, info=True) }}
    {{ log("Trimmed Model: " ~ trimmed_model, info=True) }}
    {{ log("Trim Suffix: " ~ trim_suffix, info=True) }}
    {{ log("Model Type: " ~ model_type, info=True) }}
    {{ log("", info=True) }}

    {{ log("=== Current Variable Settings ===", info=True) }}
    {{ log((trimmed_model ~ '_' ~ model_type ~ '_partition_function').upper() ~ ': ' ~ partition_function, info=True) }}
    {{ log((trimmed_model ~ '_' ~ model_type ~ '_balances').upper() ~ ': ' ~ balances, info=True) }}
    {{ log((trimmed_model ~ '_' ~ model_type ~ '_block_number').upper() ~ ': ' ~ block_number, info=True) }}
    {{ log("Uses Receipts by Hash: " ~ uses_receipts_by_hash, info=True) }}

    {{ log("", info=True) }}
    {{ log("=== Source Details ===", info=True) }}
    {{ log("Source: " ~ source('bronze_streamline', trimmed_model), info=True) }}
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
                    table_name => '{{ source( "bronze_streamline", trimmed_model) }}')
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
        {% if uses_receipts_by_hash and trimmed_model.lower().startswith('receipts') %}
            , s.value :"TX_HASH" :: STRING AS tx_hash
        {% endif %}
        FROM
            {{ source(
                "bronze_streamline",
                trimmed_model
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
{% endif %}