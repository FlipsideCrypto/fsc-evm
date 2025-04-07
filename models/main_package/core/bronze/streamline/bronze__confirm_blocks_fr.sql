{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze','core','confirm_blocks','phase_1']
) }}

{# Main query starts here #}
{{ streamline_external_table_query_fr(
    source_name = 'confirm_blocks'
) }}