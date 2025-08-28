{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze','balances','phase_4']
) }}

{# Main query starts here #}
{{ streamline_external_table_query(
    source_name = 'balances_erc20'
) }}