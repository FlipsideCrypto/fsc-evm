{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}
{# Protocol-specific addresses #}
{% set pool_address = vars.PROTOCOL_AAVE_V3_POOL_GNOSIS %}
{% set incentives_controller = vars.PROTOCOL_AAVE_INCENTIVES_CONTROLLER_GNOSIS %}


{{aave_liquidation_revenue('gnosis', 'Aave V3', pool_address)}}