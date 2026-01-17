{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'pool_id', 'token_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'balancer', 'v2', 'gnosis', 'tvl', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Balancer V2 Gnosis TVL by Pool and Token

    Tracks total value locked per pool per token.
    Note: Requires get_balancer_v2_tvl_by_pool_and_token macro implementation.
#}

{{ get_balancer_v2_tvl_by_pool_and_token(
    'gnosis',
    is_incremental(),
    vars.CURATED_LOOKBACK_HOURS,
    vars.CURATED_LOOKBACK_DAYS
) }}
