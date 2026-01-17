{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['tx_hash', 'event_index'],
    cluster_by = ['block_timestamp::date'],
    tags = ['silver_protocols', 'balancer', 'v1', 'swaps', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Balancer V1 Ethereum BPool Swaps

    Tracks LOG_SWAP events from Balancer V1 pools.
    Joins with BPools model to get pool addresses.
#}

WITH base AS (
    SELECT
        t1.event_name,
        t1.origin_from_address,
        t1.origin_to_address,
        t2.pool AS pool,
        t1.tx_hash,
        t1.event_index,
        t1.block_number,
        t1.decoded_log:caller::STRING AS caller,
        TRY_CAST(t1.decoded_log:tokenAmountIn::STRING AS NUMBER) AS tokenAmountIn,
        TRY_CAST(t1.decoded_log:tokenAmountOut::STRING AS NUMBER) AS tokenAmountOut,
        t1.decoded_log:tokenIn::STRING AS tokenIn,
        t1.decoded_log:tokenOut::STRING AS tokenOut,
        t1.block_timestamp,
        TRUNC(t1.block_timestamp, 'hour') AS hour
    FROM {{ ref('core__ez_decoded_event_logs') }} t1
    INNER JOIN {{ ref('silver_protocols__balancer_v1_ethereum_bpools') }} t2
        ON LOWER(t1.contract_address) = LOWER(t2.pool)
    WHERE t1.event_name = 'LOG_SWAP'
    {% if is_incremental() %}
        AND t1.block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
    {% endif %}
)

SELECT
    *,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM base
