{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}
{# Protocol-specific addresses #}
{% set pool_address = vars.PROTOCOL_AAVE_V3_POOL_ARBITRUM %}
{% set incentives_controller = vars.PROTOCOL_AAVE_INCENTIVES_CONTROLLER_ARBITRUM %}


{{ aave_v3_ecosystem_incentives('arbitrum', incentives_controller, 'AAVE V3')}}