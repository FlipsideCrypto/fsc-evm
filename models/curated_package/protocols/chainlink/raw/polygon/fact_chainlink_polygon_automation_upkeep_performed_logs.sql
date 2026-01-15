{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ chainlink_logs('polygon', ('0xcaacad83e47cc45c280d487ec84184eee2fa3b54ebaa393bda7549f13da228f6', '0xad8cc9579b21dfe2c2f6ea35ba15b656e46b4f5b0cb424f52739b8ce5cac9c5b'))}}