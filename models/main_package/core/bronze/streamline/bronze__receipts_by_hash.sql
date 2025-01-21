{# Set variables #}
{% set source_name = 'RECEIPTS_BY_HASH' %}
{% set source_version = '' %}
{% set model_type = '' %}

{%- set default_vars = set_default_variables_bronze(source_name, model_type) -%}

{% set partition_function = default_vars['partition_function'] %}
{% set partition_join_key = default_vars['partition_join_key'] %}
{% set balances = default_vars['balances'] %}
{% set block_number = default_vars['block_number'] %}
{% set uses_receipts_by_hash = default_vars['uses_receipts_by_hash'] %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze_receipts_by_hash']
) }}

{# Main query starts here #}
{{ streamline_external_table_query(
    source_name = source_name.lower(),
    source_version = source_version.lower(),
    partition_function = partition_function,
    balances = balances,
    block_number = block_number,
    uses_receipts_by_hash = uses_receipts_by_hash
) }}