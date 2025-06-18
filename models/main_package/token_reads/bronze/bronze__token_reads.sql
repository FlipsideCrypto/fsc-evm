{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze','token_reads','phase_2']
) }}

{# Main query starts here #}
{{ streamline_external_table_query(
    source_name = 'token_reads',
    block_number = false,
    contract_address = true,
    data_not_null = false
) }}