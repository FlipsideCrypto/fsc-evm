{% set source_name = var('BLOCKS_SOURCE_NAME', 'BLOCKS') %}
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

{{ streamline_external_table_query(
    model = "source_name.lower()",
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER)"
) }}
