{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Protocol-specific addresses #}
{% set pool_address = vars.PROTOCOL_AAVE_V2_POOL_ETHEREUM %}
{% set collector_address = vars.PROTOCOL_AAVE_COLLECTOR_V2_ETHEREUM %}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'aave', 'deposits_borrows_lender_revenue', 'curated']
) }}

{{ aave_deposits_borrows_lender_revenue(
    'ethereum',
    'AAVE V2',
    pool_address,
    collector_address,
    'raw_aave_v2_ethereum_rpc_data',
    is_incremental(),
    vars.CURATED_LOOKBACK_HOURS,
    vars.CURATED_LOOKBACK_DAYS
) }}
