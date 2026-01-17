{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'liquity', 'treasury', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Liquity Treasury Balance

    Tracks token balances in the Liquity Treasury address:
    - 0xF06016D822943C42e3Cb7FC3a6A3B1889C1045f8
    Active since: March 17, 2021

    Returns: date, chain, contract_address, token, native_balance, usd_balance
#}

{{ get_treasury_balance('ethereum', '0xF06016D822943C42e3Cb7FC3a6A3B1889C1045f8', '2021-03-17') }}
