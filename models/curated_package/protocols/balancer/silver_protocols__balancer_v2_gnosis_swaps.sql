{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['tx_hash', 'event_index'],
    cluster_by = ['block_timestamp::date'],
    tags = ['silver_protocols', 'balancer', 'v2', 'gnosis', 'swaps', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Balancer V2 Gnosis Swaps

    Tracks swap events from Balancer V2 pools.
    Note: Requires get_balancer_v2_swaps macro implementation.
#}

{{ get_balancer_v2_swaps(
    'gnosis',
    is_incremental(),
    vars.CURATED_LOOKBACK_HOURS,
    vars.CURATED_LOOKBACK_DAYS
) }}
