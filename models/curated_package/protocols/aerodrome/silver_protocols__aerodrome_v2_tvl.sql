{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'pool_address', 'token_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'aerodrome', 'v2_tvl', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Aerodrome V2 TVL

    Tracks daily TVL for Aerodrome V2 concentrated liquidity pools (Slipstream) including:
    - Pool token balances
    - USD values for each token
    - Data aggregated by date, pool, and token
#}

WITH pools AS (
    SELECT
        pool_address AS pool
        , token0_address
        , token1_address
        , tick_spacing
    FROM {{ ref('silver_protocols__aerodrome_v2_pools') }}
),

dates AS (
    SELECT DISTINCT
        DATE(hour) AS date
    FROM {{ ref('price__ez_prices_hourly') }}
    WHERE hour > DATE('2024-04-01')
    {% if is_incremental() %}
        AND DATE(hour) >= (SELECT MAX(date) - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days' FROM {{ this }})
    {% endif %}
),

sparse_balances AS (
    SELECT
        DATE(block_timestamp) AS date
        , address AS pool
        , b.contract_address
        , decimals AS decimals_adj
        , MAX_BY(balance_token / POW(10, COALESCE(decimals_adj, 18)), block_timestamp) AS balance_daily
        , MAX(block_number) AS block_number
        , MAX(b.modified_timestamp) AS modified_timestamp
    FROM PC_DBT_DB.PROD.fact_base_address_balances_by_token b
    LEFT JOIN {{ ref('price__ez_asset_metadata') }} t
        ON t.token_address = b.contract_address
    WHERE 1 = 1
        AND LOWER(address) IN (SELECT pool FROM pools)
    {% if is_incremental() %}
        AND b.modified_timestamp >= (
            SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
            FROM {{ this }}
        )
        AND b.modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}
    GROUP BY 1, 2, 3, 4
),

full_balances AS (
    SELECT
        d.date
        , ta.pool
        , ta.contract_address
        , COALESCE(
            LAST_VALUE(sb.balance_daily) IGNORE NULLS OVER (
                PARTITION BY ta.pool, ta.contract_address
                ORDER BY d.date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ),
            0
        ) AS balance_daily
        , sb.block_number
        , sb.modified_timestamp
    FROM dates d
    CROSS JOIN (SELECT DISTINCT pool, contract_address FROM sparse_balances) ta
    LEFT JOIN sparse_balances sb
        ON d.date = sb.date
        AND ta.pool = sb.pool
        AND ta.contract_address = sb.contract_address
),

full_table AS (
    SELECT
        fb.date
        , fb.pool
        , fb.contract_address
        , CASE
            WHEN contract_address = 'native_token' THEN native_token.symbol
            ELSE p.symbol
        END AS symbol_adj
        , fb.balance_daily AS balance_daily
        , CASE
            WHEN contract_address = 'native_token' THEN COALESCE(native_token.price, 0)
            ELSE COALESCE(p.price, 0)
        END AS price_adj
        , fb.balance_daily * COALESCE(price_adj, 0) AS usd_balance
        , fb.block_number
        , fb.modified_timestamp
    FROM full_balances fb
    LEFT JOIN {{ ref('price__ez_prices_hourly') }} p
        ON p.hour = fb.date
        AND fb.contract_address = p.token_address
    -- left join native token price
    LEFT JOIN {{ ref('price__ez_prices_hourly') }} native_token
        ON native_token.hour = fb.date
        AND (LOWER(native_token.token_address) IS NULL AND fb.contract_address = 'native_token')
    WHERE symbol_adj IS NOT NULL
)

SELECT
    date
    , 'base' AS chain
    , 'v2' AS version
    , pool AS pool_address
    , contract_address AS token_address
    , symbol_adj AS token_symbol
    , SUM(balance_daily) AS token_balance
    , SUM(usd_balance) AS token_balance_usd
    , MAX(block_number) AS block_number
    , MAX(modified_timestamp) AS modified_timestamp
    , SYSDATE() AS inserted_timestamp
    , '{{ invocation_id }}' AS _invocation_id
FROM full_table
WHERE usd_balance > 100
    AND usd_balance < 1e10 -- 10B
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1 DESC
