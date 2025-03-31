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
{{ streamline_external_table_query(
    source_name = vars.MAIN_SL_BRONZE_RECEIPTS_SOURCE_NAME
) }}