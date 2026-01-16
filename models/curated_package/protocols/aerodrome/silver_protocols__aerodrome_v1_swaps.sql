{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['tx_hash', 'pool_address', 'block_timestamp'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_protocols', 'aerodrome', 'v1_swaps', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Aerodrome V1 Swaps

    Tracks swap events on Aerodrome V1 pools (AMM pools) including:
    - Swap details (tokens in/out, amounts)
    - Fee calculations (swap fee, protocol revenue, supply side revenue)
    - Price data for USD conversion
#}

WITH aerodrome_pools AS (
    SELECT
        DECODED_LOG:pool::STRING AS pool_address
        , DECODED_LOG:token0::STRING AS token0_address
        , DECODED_LOG:token1::STRING AS token1_address
        , DECODED_LOG:stable::BOOLEAN AS is_stable_pool
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE CONTRACT_ADDRESS ILIKE '0x420DD381b31aEf6683db6B902084cB0FFECe40Da'
        AND EVENT_NAME ILIKE 'PoolCreated'
),

pool_fees AS (
    WITH custom_fees AS (
        SELECT
            DECODED_LOG:pool::STRING AS pool
            , DECODED_LOG:fee::INTEGER / 100000 AS fee_rate
            , block_timestamp
            , ROW_NUMBER() OVER (PARTITION BY DECODED_LOG:pool::STRING ORDER BY block_timestamp DESC) AS rn
        FROM {{ ref('core__ez_decoded_event_logs') }}
        WHERE LOWER(contract_address) = '0x420dd381b31aef6683db6b902084cb0ffece40da'
            AND event_name = 'SetCustomFee'
    )
    SELECT
        p.pool_address AS pool
        , p.is_stable_pool
        , COALESCE(
            cf.fee_rate,
            CASE
                WHEN p.is_stable_pool THEN 0.0005
                ELSE 0.003
            END
        ) AS fee_rate
        , cf.block_timestamp
        , cf.rn
    FROM aerodrome_pools p
    LEFT JOIN custom_fees cf
        ON p.pool_address = cf.pool
        AND (cf.rn = 1 OR cf.rn IS NULL)
),

swap_events AS (
    SELECT
        e.BLOCK_TIMESTAMP
        , e.block_number
        , 'Aerodrome' AS app
        , 'DeFi' AS category
        , 'Base' AS chain
        , '1' AS version
        , e.TX_HASH
        , e.ORIGIN_FROM_ADDRESS AS sender
        , DECODED_LOG:to::STRING AS recipient
        , e.CONTRACT_ADDRESS AS pool_address
        , CASE
            WHEN TRY_CAST(DECODED_LOG:amount0In::STRING AS DECIMAL(38, 0)) > 0 THEN p.token0_address
            ELSE p.token1_address
        END AS token_in_address
        , CASE
            WHEN TRY_CAST(DECODED_LOG:amount0Out::STRING AS DECIMAL(38, 0)) > 0 THEN p.token0_address
            ELSE p.token1_address
        END AS token_out_address
        , CASE
            WHEN TRY_CAST(DECODED_LOG:amount0In::STRING AS DECIMAL(38, 0)) > 0 THEN DECODED_LOG:amount0In::STRING
            ELSE DECODED_LOG:amount1In::STRING
        END AS amount_in_native
        , CASE
            WHEN TRY_CAST(DECODED_LOG:amount0Out::STRING AS DECIMAL(38, 0)) > 0 THEN DECODED_LOG:amount0Out::STRING
            ELSE DECODED_LOG:amount1Out::STRING
        END AS amount_out_native
        , f.fee_rate AS swap_fee_pct
        , f.is_stable_pool
        , e.modified_timestamp
    FROM {{ ref('core__ez_decoded_event_logs') }} e
    INNER JOIN aerodrome_pools p
        ON e.CONTRACT_ADDRESS = p.pool_address
    LEFT JOIN pool_fees f
        ON p.pool_address = f.pool
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
