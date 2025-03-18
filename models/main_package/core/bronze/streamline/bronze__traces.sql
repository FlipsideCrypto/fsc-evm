{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = get_path_tags(model)
) }}

{# Main query starts here #}
{{ streamline_external_table_query(
    source_name = 'traces'
) }}