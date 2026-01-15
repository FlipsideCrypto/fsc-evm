{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}
{# Protocol-specific addresses #}
{% set pool_address = vars.PROTOCOL_AAVE_V3_POOL_ETHEREUM %}
{% set incentives_controller = vars.PROTOCOL_AAVE_INCENTIVES_CONTROLLER_V3_ETHEREUM %}{% set collector_address = vars.PROTOCOL_AAVE_COLLECTOR_V3_ETHEREUM %}



{{ aave_deposits_borrows_lender_revenue('ethereum', 'AAVE V3', pool_address, collector_address, 'raw_aave_v3_ethereum_rpc_data')}}