{# Set variables #}
{% set source_name = 'DECODED_TRACES' %}
{% set source_version = '' %}
{% set model_type = 'FR' %}

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
    tags = ['bronze_decoded_traces','bronze_streamline_v1']
) }}

{# Main query starts here #}
{{ streamline_external_table_query_decoder_fr(
    source_name = source_name.lower(),
    source_version = source_version.lower()
) }}