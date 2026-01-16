{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['pool_address'],
    cluster_by = ['pool_address'],
    tags = ['silver_protocols', 'aerodrome', 'v2_pools', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Aerodrome V2 Pools

    Reference table for Aerodrome V2 concentrated liquidity pools (Slipstream) including:
    - Pool address and token pair
    - Tick spacing configuration
    Factory address: 0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A
#}

SELECT
    DECODED_LOG:pool::STRING AS pool_address
    , DECODED_LOG:token0::STRING AS token0_address
    , DECODED_LOG:token1::STRING AS token1_address
    , DECODED_LOG:tickSpacing::INTEGER AS tick_spacing
    , block_number
    , modified_timestamp
    , SYSDATE() AS inserted_timestamp
    , '{{ invocation_id }}' AS _invocation_id
FROM {{ ref('core__ez_decoded_event_logs') }}
WHERE CONTRACT_ADDRESS = LOWER('0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A')
    AND EVENT_NAME = 'PoolCreated'
{% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
{% endif %}
