{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['pool_id'],
    cluster_by = ['block_timestamp::date'],
    tags = ['silver_protocols', 'balancer', 'v2', 'polygon', 'pool_metadata', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Balancer V2 Polygon Pool Metadata

    Tracks pool registration and metadata.
    Note: Requires get_balancer_v2_pool_metadata macro implementation.
#}

{{ get_balancer_v2_pool_metadata(
    'polygon',
    is_incremental(),
    vars.CURATED_LOOKBACK_HOURS,
    vars.CURATED_LOOKBACK_DAYS
) }}
