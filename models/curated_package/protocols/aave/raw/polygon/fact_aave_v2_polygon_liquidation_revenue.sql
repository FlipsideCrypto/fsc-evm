{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}
{# Protocol-specific addresses #}
{% set pool_address = vars.PROTOCOL_AAVE_V2_POOL_POLYGON %}
{% set incentives_controller = vars.PROTOCOL_AAVE_INCENTIVES_CONTROLLER_POLYGON %}


{{aave_liquidation_revenue('polygon', 'Aave V2', pool_address)}}