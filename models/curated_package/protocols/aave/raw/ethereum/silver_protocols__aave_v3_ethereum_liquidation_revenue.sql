{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Protocol-specific addresses #}
{% set pool_address = vars.PROTOCOL_AAVE_V3_POOL_ETHEREUM %}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = '_log_id',
    cluster_by = ['date'],
    tags = ['silver_protocols', 'aave', 'liquidation_revenue', 'curated']
) }}

{{ aave_liquidation_revenue(
    'ethereum',
    'Aave V3',
    pool_address,
    is_incremental(),
    vars.CURATED_LOOKBACK_HOURS,
    vars.CURATED_LOOKBACK_DAYS
) }}
