{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze','balances','erc20','phase_4']
) }}

{# Main query starts here #}
{{ streamline_external_table_query(
    source_name = 'balances_erc20',
    contract_address = true
) }}