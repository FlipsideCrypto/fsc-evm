{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ chainlink_logs('bsc', ('0xaffc45517195d6499808c643bd4a7b0ffeedf95bea5852840d7bfcf63f59e821'))}}