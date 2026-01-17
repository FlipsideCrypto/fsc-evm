{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_timestamp', 'pool'],
    cluster_by = ['block_timestamp::date'],
    tags = ['silver_protocols', 'balancer', 'v1', 'pools', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Balancer V1 Ethereum BPools

    Tracks Balancer V1 pool creation events from the Core Pool Factory.
    Factory address: 0x9424B1412450D0f8Fc2255FAf6046b98213B76Bd
#}

WITH base AS (
    SELECT
        block_timestamp,
        event_name,
        event_index,
        decoded_log:caller::STRING AS caller,
        decoded_log:pool::STRING AS pool
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE contract_address = LOWER('0x9424B1412450D0f8Fc2255FAf6046b98213B76Bd') -- Balancer v1 Core Pool Factory address
        AND event_name = 'LOG_NEW_POOL'
    {% if is_incremental() %}
        AND block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
    {% endif %}
)

SELECT
    *,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM base
