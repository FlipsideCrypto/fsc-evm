{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze_decoded_logs']
) }}

{# Main query starts here #}
{{ streamline_external_table_query_decoder(
    source_name = 'decoded_logs',
    source_version = ''
) }}