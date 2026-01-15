{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}
{# Protocol-specific addresses #}
{% set pool_address = vars.PROTOCOL_AAVE_V2_POOL_AVALANCHE %}
{% set incentives_controller = vars.PROTOCOL_AAVE_INCENTIVES_CONTROLLER_V2_AVALANCHE %}{% set collector_address = vars.PROTOCOL_AAVE_COLLECTOR_AVALANCHE %}



{{ aave_deposits_borrows_lender_revenue('avalanche', 'AAVE V2', pool_address, collector_address, 'raw_aave_v2_avalanche_rpc_data')}}