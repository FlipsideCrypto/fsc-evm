{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'pool_address', 'token_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'aerodrome', 'v1_tvl', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Aerodrome V1 TVL

    Tracks daily TVL for Aerodrome V1 pools (AMM pools) including:
    - Pool token balances (token0 and token1)
    - USD values for each token
    - Data is unpivoted to show one row per token per pool per day
#}

WITH pools AS (
    SELECT
        DECODED_LOG:pool::STRING AS pool
        , DECODED_LOG:token0::STRING AS token0
        , DECODED_LOG:token1::STRING AS token1
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE LOWER(contract_address) = '0x420dd381b31aef6683db6b902084cb0ffece40da'
        AND event_name = 'PoolCreated'
),

daily_reserves AS (
    SELECT
        block_timestamp::DATE AS date
        , CONTRACT_ADDRESS AS pool_address
        , TRY_TO_NUMBER(DECODED_LOG:reserve0::STRING) AS token0_reserve
        , TRY_TO_NUMBER(DECODED_LOG:reserve1::STRING) AS token1_reserve
        , block_number
        , modified_timestamp
        , ROW_NUMBER() OVER (
            PARTITION BY CONTRACT_ADDRESS, DATE_TRUNC('day', block_timestamp)
            ORDER BY block_timestamp DESC
        ) AS rn
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE CONTRACT_ADDRESS IN (SELECT pool FROM pools)
        AND EVENT_NAME = 'Sync'
    {% if is_incremental() %}
        AND modified_timestamp >= (
            SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
            FROM {{ this }}
        )
        AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}
),

all_data AS (
    SELECT
        r.date
        , 'base' AS chain
        , 'v1' AS version
        , p.pool AS pool_address
        , p.token0
        , t0.symbol AS token0_symbol
        , p.token1
        , t1.symbol AS token1_symbol
        , r.token0_reserve / POW(10, COALESCE(t0.DECIMALS, 18)) AS token0_balance
        , r.token1_reserve / POW(10, COALESCE(t1.DECIMALS, 18)) AS token1_balance
        , (token0_balance * COALESCE(t0.price, 0)) AS token0_usd
        , (token1_balance * COALESCE(t1.price, 0)) AS token1_usd
        , (r.token0_reserve / POW(10, COALESCE(t0.DECIMALS, 18))) * COALESCE(t0.price, 0) +
          (r.token1_reserve / POW(10, COALESCE(t1.DECIMALS, 18))) * COALESCE(t1.price, 0) AS tvl_usd
        , r.block_number
        , r.modified_timestamp
    FROM pools p
    LEFT JOIN daily_reserves r
        ON p.pool = r.pool_address
    LEFT JOIN {{ ref('price__ez_prices_hourly') }} t0
        ON r.date = t0.hour AND p.token0 = t0.token_address
    LEFT JOIN {{ ref('price__ez_prices_hourly') }} t1
        ON r.date = t1.hour AND p.token1 = t1.token_address
    WHERE r.rn = 1
        AND ((r.token0_reserve / POW(10, COALESCE(t0.DECIMALS, 18))) * COALESCE(t0.price, 0)) +
            ((r.token1_reserve / POW(10, COALESCE(t1.DECIMALS, 18))) * COALESCE(t1.price, 0)) < 1e11
)

SELECT
    date
    , chain
    , version
    , pool_address
    , token0 AS token_address
    , token0_symbol AS token_symbol
    , token0_balance AS token_balance
    , token0_usd AS token_balance_usd
    , block_number
    , modified_timestamp
    , SYSDATE() AS inserted_timestamp
    , '{{ invocation_id }}' AS _invocation_id
FROM all_data

UNION ALL

SELECT
    date
    , chain
    , version
    , pool_address
    , token1 AS token_address
    , token1_symbol AS token_symbol
    , token1_balance AS token_balance
    , token1_usd AS token_balance_usd
    , block_number
    , modified_timestamp
    , SYSDATE() AS inserted_timestamp
    , '{{ invocation_id }}' AS _invocation_id
FROM all_data
