{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'lido', 'dao_treasury', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Protocol-specific addresses #}
{% set treasury_address = vars.PROTOCOL_LIDO_TREASURY %}

{#
    Lido DAO Treasury

    Tracks token balances in the Lido DAO Treasury address.
    Treasury address: 0x3e40d73eb977dc6a537af587d48316fee66e9c8c
    Active since: December 17, 2020
#}

{{ get_treasury_balance(
    'ethereum',
    treasury_address,
    '2020-12-17',
    is_incremental(),
    vars.CURATED_LOOKBACK_HOURS,
    vars.CURATED_LOOKBACK_DAYS
) }}
