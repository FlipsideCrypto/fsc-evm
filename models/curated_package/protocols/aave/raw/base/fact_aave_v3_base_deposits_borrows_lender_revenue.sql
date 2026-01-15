{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}
{# Protocol-specific addresses #}
{% set pool_address = vars.PROTOCOL_AAVE_V3_POOL_BASE %}
{% set incentives_controller = vars.PROTOCOL_AAVE_INCENTIVES_CONTROLLER_BASE %}{% set collector_address = vars.PROTOCOL_AAVE_COLLECTOR_BASE %}



{{ aave_deposits_borrows_lender_revenue('base', 'AAVE V3', pool_address, collector_address, 'raw_aave_v3_base_rpc_data')}}