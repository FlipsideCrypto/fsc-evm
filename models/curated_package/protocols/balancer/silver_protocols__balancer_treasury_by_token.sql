{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'balancer', 'treasury', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Balancer Treasury by Token

    Tracks token balances in Balancer treasury addresses:
    - Protocol Fee Collector: 0xce88686553686da562ce7cea497ce749da109f9f
    - DAO Multisig: 0x10a19e7ee7d7f8a52822f6817de8ea18204f2e4f
    - Treasury: 0xb618f903ad1d00d6f7b92f5b0954dcdc056fc533
    - LM Multisig: 0x0efccbb9e2c09ea29551879bd9da32362b32fc89
    Earliest date: 2020-06-23
#}

WITH base AS (
    {{ get_treasury_balance(
        chain='ethereum',
        addresses=[
            '0xce88686553686da562ce7cea497ce749da109f9f',
            '0x10a19e7ee7d7f8a52822f6817de8ea18204f2e4f',
            '0xb618f903ad1d00d6f7b92f5b0954dcdc056fc533',
            '0x0efccbb9e2c09ea29551879bd9da32362b32fc89'
        ],
        earliest_date='2020-06-23',
        is_incremental_run=is_incremental(),
        lookback_hours=vars.CURATED_LOOKBACK_HOURS,
        lookback_days=vars.CURATED_LOOKBACK_DAYS
    ) }}
)

SELECT
    *,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM base
