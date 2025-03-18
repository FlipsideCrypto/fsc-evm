{# Set variables #}
{% set source_name = 'DECODED_TRACES' %}
{% set source_version = 'V2' if get_var('GLOBAL_SL_STREAMLINE_V1_ENABLED', false) else '' %}
{% set model_type = '' %}

{%- set default_vars = set_default_variables_bronze(source_name, model_type) -%}

{# Log configuration details #}
{{ log_model_details(
    vars = default_vars
) }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = get_path_tags(model)
) }}

{# Main query starts here #}
{{ streamline_external_table_query_decoder(
    source_name = source_name.lower(),
    source_version = source_version.lower()
) }}