{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze','abis','phase_2']
) }}

{# Main query starts here #}
{{ streamline_external_table_query_fr(
    source_name = 'contract_abis',
    block_number = false,
    contract_address = true
) }}