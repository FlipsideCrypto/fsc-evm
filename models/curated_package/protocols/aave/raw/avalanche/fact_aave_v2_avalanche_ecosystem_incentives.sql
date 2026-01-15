{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}
{# Protocol-specific addresses #}
{% set pool_address = vars.PROTOCOL_AAVE_V2_POOL_AVALANCHE %}
{% set incentives_controller = vars.PROTOCOL_AAVE_INCENTIVES_CONTROLLER_V2_AVALANCHE %}


{{ aave_v2_ecosystem_incentives('avalanche', incentives_controller, 'AAVE V2')}}