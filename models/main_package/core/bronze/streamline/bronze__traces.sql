{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze','core','phase_1']
) }}

{# Main query starts here #}
{{ streamline_external_table_query(
    source_name = 'traces'
) }}