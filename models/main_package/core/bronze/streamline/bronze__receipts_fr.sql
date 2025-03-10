{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze_receipts']
) }}

{# Main query starts here #}
{{ streamline_external_table_query_fr(
    source_name = 'receipts',
    source_version = '',
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER)",
    partition_join_key = 'partition_key',
    balances = false,
    block_number = true
) }}