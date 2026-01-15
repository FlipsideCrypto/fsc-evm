{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ chainlink_ocr_rewards_daily('arbitrum') }}