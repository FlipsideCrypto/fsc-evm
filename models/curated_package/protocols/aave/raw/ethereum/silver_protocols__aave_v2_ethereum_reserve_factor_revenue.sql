{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}
{# Protocol-specific addresses #}
{% set pool_address = vars.PROTOCOL_AAVE_V2_POOL_ETHEREUM %}
{% set incentives_controller = vars.PROTOCOL_AAVE_INCENTIVES_CONTROLLER_ETHEREUM %}{% set collector_address = vars.PROTOCOL_AAVE_COLLECTOR_V2_ETHEREUM %}



{{ aave_v2_reserve_factor_revenue('ethereum', pool_address, 'AAVE V2')}}