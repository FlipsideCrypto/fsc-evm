{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['tx_hash', 'event_index'],
    cluster_by = ['block_timestamp::date'],
    tags = ['silver_protocols', 'balancer', 'v2', 'gnosis', 'swap_fee_changes', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Balancer V2 Gnosis Swap Fee Changes

    Tracks SwapFeePercentageChanged events for pools.
    Note: Requires get_balancer_v2_swap_fee_changes macro implementation.
#}

{{ get_balancer_v2_swap_fee_changes(
    'gnosis',
    is_incremental(),
    vars.CURATED_LOOKBACK_HOURS,
    vars.CURATED_LOOKBACK_DAYS
) }}
