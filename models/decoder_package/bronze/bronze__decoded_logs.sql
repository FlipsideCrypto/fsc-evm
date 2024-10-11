{# Set variables #}
{% set source_name = 'DECODED_LOGS' %}
{% set source_version = var('BRONZE_DECODED_LOGS_SOURCE_VERSION', '') %}
{% set model_type = '' %}

{%- set default_vars = set_default_variables_bronze(source_name, model_type) -%}

{# Log configuration details #}
{{ log_bronze_details(
    source_name = source_name,
    source_version = source_version,
    model_type = model_type
) }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['decoder']
) }}

{# Main query starts here #}
{{ streamline_external_table_query_decoder(
    source_name = source_name.lower(),
    source_version = source_version.lower()
) }}