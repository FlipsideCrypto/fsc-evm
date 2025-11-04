{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze','core','phase_1']
) }}

{# Main query starts here #}
{{ streamline_external_table_query_fr(
    source_name = 'traces',
    error_code = vars.MAIN_CORE_BRONZE_BLOCKS_ERROR_CODE_ENABLED
) }}