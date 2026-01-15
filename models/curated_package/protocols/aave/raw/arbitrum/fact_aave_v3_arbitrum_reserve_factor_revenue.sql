{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}
{# Protocol-specific addresses #}
{% set pool_address = vars.PROTOCOL_AAVE_V3_POOL_ARBITRUM %}
{% set incentives_controller = vars.PROTOCOL_AAVE_INCENTIVES_CONTROLLER_ARBITRUM %}{% set collector_address = vars.PROTOCOL_AAVE_COLLECTOR_ARBITRUM %}



{{ aave_v3_reserve_factor_revenue('arbitrum', pool_address, 'AAVE V3')}}