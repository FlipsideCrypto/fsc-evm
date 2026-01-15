{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}
{# Protocol-specific addresses #}
{% set pool_address = vars.PROTOCOL_AAVE_V3_POOL_BASE %}
{% set incentives_controller = vars.PROTOCOL_AAVE_INCENTIVES_CONTROLLER_BASE %}{% set collector_address = vars.PROTOCOL_AAVE_COLLECTOR_BASE %}



{{ aave_v3_reserve_factor_revenue('base', pool_address, 'AAVE V3')}}