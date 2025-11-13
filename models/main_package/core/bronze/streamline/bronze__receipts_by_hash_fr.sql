{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze','core','receipts_by_hash','phase_1']
) }}

{# Main query starts here #}
{{ streamline_external_table_query_fr(
    source_name = 'receipts_by_hash',
    tx_hash = vars.MAIN_CORE_RECEIPTS_BY_HASH_ENABLED,
    error_code = vars.MAIN_CORE_BRONZE_BLOCKS_ERROR_CODE_ENABLED
) }}