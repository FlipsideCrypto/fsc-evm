{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze_abis']
) }}

{# Main query starts here #}
{{ streamline_external_table_query(
    source_name = 'contract_abis',
    contract_address = true
) }}