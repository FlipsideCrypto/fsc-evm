{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze_core']
) }}

{# Main query starts here #}
{{ streamline_external_table_query_fr(
    source_name = 'confirm_blocks',
    source_version = '',
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER)",
    balances = false,
    block_number = true
) }}