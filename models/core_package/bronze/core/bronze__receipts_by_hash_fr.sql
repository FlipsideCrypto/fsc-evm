{% set uses_receipts_by_hash = var('GLOBAL_USES_RECEIPTS_BY_HASH', false) %}

{% if uses_receipts_by_hash %}
{% set source_name = 'RECEIPTS_BY_HASH' %}
{% set source_version = '' %}
{% set model_type = '' %}

{%- set default_vars = set_default_variables_bronze(source_name, model_type) -%}

{{ log_bronze_details(
    source_name = source_name,
    source_version = source_version,
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
    source_name = source_name.lower(),
    source_version = source_version.lower(),
    partition_function = default_vars['partition_function'],
    partition_join_key = default_vars['partition_join_key'],
    balances = default_vars['balances'],
    block_number = default_vars['block_number'],
    uses_receipts_by_hash = default_vars['uses_receipts_by_hash']
) }}
{% endif %}