{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze','decoded_logs','phase_3']
) }}

{# Main query starts here #}
{{ streamline_external_table_query_decoder(
    source_name = 'decoded_logs'
) }}