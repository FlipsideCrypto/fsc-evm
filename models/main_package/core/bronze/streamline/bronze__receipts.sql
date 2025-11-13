{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze','core','receipts','phase_1']
) }}

{# Main query starts here #}
{{ streamline_external_table_query(
    source_name = 'receipts',
    error_code = vars.MAIN_CORE_BRONZE_BLOCKS_ERROR_CODE_ENABLED
) }}