{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = get_path_tags(model)
) }}

{# Main query starts here #}
{{ streamline_external_table_query(
    source_name = 'receipts_by_hash',
    tx_hash = vars.MAIN_SL_RECEIPTS_BY_HASH_ENABLED
) }}