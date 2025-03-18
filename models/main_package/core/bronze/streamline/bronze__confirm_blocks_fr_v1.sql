{# Set variables #}
{% set source_name = 'CONFIRM_BLOCKS' %}
{% set source_version = '' %}
{% set model_type = 'FR' %}

{%- set default_vars = set_default_variables_bronze(source_name, model_type) -%}

{% set partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER)" %}
{% set partition_join_key = '_partition_by_block_id' %}
{% set balances = default_vars['balances'] %}
{% set block_number = false %}
{% set uses_receipts_by_hash = default_vars['uses_receipts_by_hash'] %}

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
{{ streamline_external_table_query_fr(
    source_name = source_name.lower(),
    source_version = source_version.lower(),
    partition_function = partition_function,
    partition_join_key = partition_join_key,
    balances = balances,
    block_number = block_number,
    uses_receipts_by_hash = uses_receipts_by_hash
) }}
