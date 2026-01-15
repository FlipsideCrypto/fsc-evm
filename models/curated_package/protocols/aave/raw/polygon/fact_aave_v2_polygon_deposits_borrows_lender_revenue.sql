{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}
{# Protocol-specific addresses #}
{% set pool_address = vars.PROTOCOL_AAVE_V2_POOL_POLYGON %}
{% set incentives_controller = vars.PROTOCOL_AAVE_INCENTIVES_CONTROLLER_POLYGON %}{% set collector_address = vars.PROTOCOL_AAVE_COLLECTOR_POLYGON %}



{{ aave_deposits_borrows_lender_revenue('polygon', 'AAVE V2', pool_address, collector_address, 'raw_aave_v2_polygon_rpc_data')}}