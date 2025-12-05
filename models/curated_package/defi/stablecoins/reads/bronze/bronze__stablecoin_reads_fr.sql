{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze','stablecoin_reads','phase_4']
) }}

{# Main query starts here #}
{{ streamline_external_table_query_fr(
    source_name = 'contract_reads',
    block_number = false,
    contract_address = true,
    data_not_null = false
) }}