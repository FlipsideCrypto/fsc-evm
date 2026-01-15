{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ get_balancer_v2_swap_fee_changes('polygon') }}