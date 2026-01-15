{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ chainlink_direct_operator_rewards_daily('bsc')}}