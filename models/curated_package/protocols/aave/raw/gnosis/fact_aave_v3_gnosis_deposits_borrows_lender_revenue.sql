{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}
{# Protocol-specific addresses #}
{% set pool_address = vars.PROTOCOL_AAVE_V3_POOL_GNOSIS %}
{% set incentives_controller = vars.PROTOCOL_AAVE_INCENTIVES_CONTROLLER_GNOSIS %}{% set collector_address = vars.PROTOCOL_AAVE_COLLECTOR_GNOSIS %}



{{ aave_deposits_borrows_lender_revenue('gnosis', 'AAVE V3', pool_address, collector_address, 'raw_aave_v3_gnosis_rpc_data')}}