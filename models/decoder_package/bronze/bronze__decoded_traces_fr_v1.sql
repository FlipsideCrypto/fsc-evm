{% set source_name = 'DECODED_TRACES' %}
{% set source_version = '' %}
{% set model_type = 'FR' %}

{%- set default_vars = set_default_variables_bronze(source_name, model_type) -%}

{{ log_bronze_details(
    source_name = source_name,
    source_version = source_version,
    model_type = model_type
) }}

{{ config (
    materialized = 'view',
    tags = ['streamline_core_complete', 'bronze_external']
) }}

{{ streamline_external_table_query_decoder_fr(
    source_name = source_name.lower(),
    source_version = source_version.lower()
) }}