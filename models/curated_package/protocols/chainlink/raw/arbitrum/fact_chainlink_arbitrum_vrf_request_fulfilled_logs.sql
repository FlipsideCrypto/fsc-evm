{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ chainlink_vrf_request_fulfilled_logs('arbitrum') }}