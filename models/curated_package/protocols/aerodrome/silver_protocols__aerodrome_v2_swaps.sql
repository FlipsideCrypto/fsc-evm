{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['tx_hash', 'pool_address', 'block_timestamp'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_protocols', 'aerodrome', 'v2_swaps', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Aerodrome V2 Swaps

    Tracks swap events on Aerodrome V2 concentrated liquidity pools (Slipstream) including:
    - Swap details (tokens in/out, amounts)
    - Fee calculations (swap fee, protocol revenue, supply side revenue)
    - Price data for USD conversion
    - Dynamic fee structure based on tick spacing
#}

WITH pools AS (
    SELECT
        pool_address
        , token0_address
        , token1_address
        , tick_spacing
    FROM {{ ref('silver_protocols__aerodrome_v2_pools') }}
),

pool_fees AS (
    WITH default_fees AS (
        SELECT
            pool_address
            , CASE
                WHEN tick_spacing = 1 THEN 0.0001    -- CL1: 1bp
                WHEN tick_spacing = 10 THEN 0.0005   -- CL10: 5bps
                WHEN tick_spacing = 50 THEN 0.0005   -- CL50: 5bps
                WHEN tick_spacing = 100 THEN 0.0005  -- CL100: 5bps
                WHEN tick_spacing = 200 THEN 0.003   -- CL200: 30bps
                WHEN tick_spacing = 2000 THEN 0.01   -- CL2000: 100bps
                ELSE 0.003                           -- Default
            END AS default_fee_rate
            , CASE
                WHEN tick_spacing >= 100 THEN FALSE
                ELSE TRUE
            END AS is_stable_pool
        FROM pools
    ),
    custom_fees AS (
        SELECT
            LOWER(DECODED_LOG:pool::STRING) AS pool
            , DECODED_LOG:fee::INTEGER / 1e6 AS fee_rate
            , block_timestamp
            , ROW_NUMBER() OVER (PARTITION BY DECODED_LOG:pool::STRING ORDER BY block_timestamp DESC) AS rn
        FROM {{ ref('core__ez_decoded_event_logs') }}
        WHERE CONTRACT_ADDRESS = LOWER('0xf4171b0953b52fa55462e4d76eca1845db69af00')
            AND EVENT_NAME = 'SetCustomFee'
    )
    SELECT
        df.pool_address
        , CASE
            WHEN cf.fee_rate IS NOT NULL THEN cf.fee_rate
            ELSE df.default_fee_rate
        END AS fee_rate
        , df.is_stable_pool
        , cf.block_timestamp
        , cf.rn
    FROM default_fees df
    LEFT JOIN custom_fees cf
        ON df.pool_address = cf.pool
        AND cf.rn = 1
),

swap_events AS (
    SELECT
        e.BLOCK_TIMESTAMP
        , e.block_number
        , 'Aerodrome' AS app
        , 'DeFi' AS category
        , 'Base' AS chain
        , '2' AS version
        , e.TX_HASH
        , e.ORIGIN_FROM_ADDRESS AS sender
        , DECODED_LOG:recipient::STRING AS recipient
        , e.CONTRACT_ADDRESS AS pool_address
        , f.is_stable_pool
        , CASE
            WHEN TRY_CAST(DECODED_LOG:amount0::STRING AS DECIMAL(38, 0)) < 0 THEN p.token1_address
            ELSE p.token0_address
        END AS token_in_address
        , CASE
            WHEN TRY_CAST(DECODED_LOG:amount0::STRING AS DECIMAL(38, 0)) > 0 THEN p.token1_address
            ELSE p.token0_address
        END AS token_out_address
        , CASE
            WHEN TRY_CAST(DECODED_LOG:amount0::STRING AS DECIMAL(38, 0)) > 0
            THEN ABS(TRY_CAST(DECODED_LOG:amount0::STRING AS DECIMAL(38, 0)))
            ELSE ABS(TRY_CAST(DECODED_LOG:amount1::STRING AS DECIMAL(38, 0)))
        END AS amount_in_native
        , CASE
            WHEN TRY_CAST(DECODED_LOG:amount0::STRING AS DECIMAL(38, 0)) < 0
            THEN ABS(TRY_CAST(DECODED_LOG:amount0::STRING AS DECIMAL(38, 0)))
            ELSE ABS(TRY_CAST(DECODED_LOG:amount1::STRING AS DECIMAL(38, 0)))
        END AS amount_out_native
        , COALESCE(f.fee_rate, 0.003) AS swap_fee_pct
        , e.modified_timestamp
    FROM {{ ref('core__ez_decoded_event_logs') }} e
    INNER JOIN pools p
        ON e.CONTRACT_ADDRESS = p.pool_address
    LEFT JOIN pool_fees f
        ON p.pool_address = f.pool_address
    WHERE e.EVENT_NAME ILIKE 'Swap'
    {% if is_incremental() %}
        AND e.modified_timestamp >= (
            SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
            FROM {{ this }}
        )
        AND e.modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}
),

prices_and_decimals AS (
    SELECT
        se.*
        , tin.DECIMALS AS token_in_decimals
        , tin.PRICE AS token_in_price
        , tin.SYMBOL AS token_in_symbol
        , tout.DECIMALS AS token_out_decimals
        , tout.PRICE AS token_out_price
        , tout.SYMBOL AS token_out_symbol
    FROM swap_events se
    LEFT JOIN {{ ref('price__ez_prices_hourly') }} tin
        ON se.token_in_address = tin.TOKEN_ADDRESS
        AND DATE_TRUNC('hour', se.BLOCK_TIMESTAMP) = tin.HOUR
        AND tin.blockchain = 'base'
    LEFT JOIN {{ ref('price__ez_prices_hourly') }} tout
        ON se.token_out_address = tout.TOKEN_ADDRESS
        AND DATE_TRUNC('hour', se.BLOCK_TIMESTAMP) = tout.HOUR
        AND tout.blockchain = 'base'
)

SELECT
    pd.BLOCK_TIMESTAMP
    , pd.block_number
    , pd.app
    , pd.category
    , pd.chain
    , pd.version
    , pd.TX_HASH
    , pd.sender
    , pd.recipient
    , pd.pool_address
    , pd.is_stable_pool
    , pd.token_in_address
    , pd.token_in_symbol
    , pd.token_out_address
    , pd.token_out_symbol
    , pd.amount_in_native / POW(10, COALESCE(pd.token_in_decimals, 18)) AS amount_in_native
    , TRY_CAST(pd.amount_in_native AS DECIMAL(38, 0)) / POW(10, COALESCE(pd.token_in_decimals, 18)) * pd.token_in_price AS amount_in_usd
    , pd.amount_out_native / POW(10, COALESCE(pd.token_out_decimals, 18)) AS amount_out_native
    , TRY_CAST(pd.amount_out_native AS DECIMAL(38, 0)) / POW(10, COALESCE(pd.token_out_decimals, 18)) * pd.token_out_price AS amount_out_usd
    , pd.swap_fee_pct
    , (TRY_CAST(pd.amount_in_native AS DECIMAL(38, 0)) / POW(10, COALESCE(pd.token_in_decimals, 18)) * pd.token_in_price * pd.swap_fee_pct) AS fee_usd
    , (TRY_CAST(pd.amount_in_native AS DECIMAL(38, 0)) / POW(10, COALESCE(pd.token_in_decimals, 18)) * pd.token_in_price * pd.swap_fee_pct * 0.1667) AS revenue
    , (TRY_CAST(pd.amount_in_native AS DECIMAL(38, 0)) / POW(10, COALESCE(pd.token_in_decimals, 18)) * pd.token_in_price * pd.swap_fee_pct * 0.8333) AS supply_side_revenue_usd
    , pd.modified_timestamp
    , SYSDATE() AS inserted_timestamp
    , '{{ invocation_id }}' AS _invocation_id
FROM prices_and_decimals pd
WHERE pd.token_in_decimals IS NOT NULL
    AND pd.token_out_decimals IS NOT NULL
    AND TRY_CAST(pd.amount_in_native AS DECIMAL(38, 0)) / POW(10, COALESCE(pd.token_in_decimals, 18)) * pd.token_in_price < 1e9 -- No swaps above 1B
ORDER BY pd.BLOCK_TIMESTAMP DESC
