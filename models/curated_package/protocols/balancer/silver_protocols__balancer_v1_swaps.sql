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
    Balancer V1 Swaps

    Enriched swap data with USD values and fee calculations.
    Joins with price data and swap fee configurations.
#}

WITH swap_details AS (
    SELECT
        DATE_TRUNC('day', swaps.block_timestamp) AS block_date,
        swaps.block_timestamp,
        swaps.block_number,
        origin_from_address,
        origin_to_address,
        event_index,
        tx_hash,
        swaps.hour AS hour,
        'ethereum' AS chain,
        pool,
        caller,
        tokenIn,
        tokenAmountIn,
        t2.price AS tokenInPrice,
        t2.symbol AS tokenInSymbol,
        t2.decimals AS tokenInDecimals,
        tokenOut,
        tokenAmountOut,
        t3.price AS tokenOutPrice,
        t3.symbol AS tokenOutSymbol,
        t3.decimals AS tokenOutDecimals
    FROM {{ ref('silver_protocols__balancer_v1_ethereum_bpool_swaps') }} AS swaps
    LEFT JOIN {{ ref('price__ez_prices_hourly') }} t2
        ON LOWER(swaps.tokenIn) = LOWER(t2.token_address)
        AND t2.hour = swaps.hour
    LEFT JOIN {{ ref('price__ez_prices_hourly') }} t3
        ON LOWER(swaps.tokenOut) = LOWER(t3.token_address)
        AND t3.hour = swaps.hour
    {% if is_incremental() %}
    WHERE swaps.block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
    {% endif %}
    ORDER BY block_date ASC
),

swaps_usd_raw AS (
    SELECT
        swap.block_date,
        swap.block_timestamp,
        source_fees.block_timestamp AS set_fee_block_timestamp,
        swap.block_number,
        source_fees.block_number AS fee_block_number,
        swap.event_index,
        swap.tx_hash,
        hour AS swap_hour,
        chain,
        pool,
        origin_from_address,
        origin_to_address,
        caller,
        source_fees.tx_hash AS set_fee_tx_hash,
        tokenIn,
        tokenAmountIn,
        tokenInPrice,
        tokenInSymbol,
        tokenInDecimals,
        tokenAmountIn / POW(10, tokenInDecimals) * tokenInPrice AS tokenAmountInUSD,
        tokenAmountIn / POW(10, tokenInDecimals) AS tokenAmountInNative,
        tokenOut,
        tokenAmountOut,
        tokenOutPrice,
        tokenOutSymbol,
        tokenOutDecimals,
        tokenAmountOut / POW(10, tokenOutDecimals) * tokenOutPrice AS tokenAmountOutUSD,
        tokenAmountOut / POW(10, tokenOutDecimals) AS tokenAmountOutNative,
        source_fees.decoded_input_data:swapFee / 1e18 AS swapFee,
        swapFee * tokenAmountInUSD AS swapFeeUSD,
        swapFee * tokenAmountInNative AS swapFeeNative,
        ROW_NUMBER() OVER (
            PARTITION BY source_fees.to_address, swap.tx_hash, swap.event_index
            ORDER BY source_fees.block_number DESC NULLS FIRST
        ) AS row_num
    FROM swap_details swap
    LEFT JOIN {{ ref('core__ez_decoded_traces') }} source_fees
        ON LOWER(source_fees.to_address) = LOWER(swap.pool)
        AND source_fees.block_number < swap.block_number
    WHERE source_fees.function_name = 'setSwapFee'
),

swaps_usd AS (
    SELECT *
    FROM swaps_usd_raw
    WHERE row_num = 1
)

SELECT
    block_timestamp,
    chain,
    'balancer' AS app,
    'v1' AS version,
    tx_hash,
    origin_from_address AS sender,
    origin_to_address AS recipient,
    pool AS pool_address,
    tokenAmountIn AS amount_in_native,
    tokenAmountInUSD AS amount_in_usd,
    tokenInSymbol AS token_in_symbol,
    tokenIn AS token_in_address,
    tokenAmountOut AS amount_out_native,
    tokenAmountOutUSD AS amount_out_usd,
    tokenOutSymbol AS token_out_symbol,
    tokenOut AS token_out_address,
    swapFee AS swap_fee_pct,
    swapFeeUSD AS fee_usd,
    swapFeeNative AS fee_native,
    0 AS treasury_cash_flow,
    0 AS treasury_cash_flow_native,
    0 AS vebal_cash_flow,
    0 AS vebal_cash_flow_native,
    swapFeeUSD AS service_cash_flow,
    swapFeeNative AS service_cash_flow_native,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM swaps_usd
