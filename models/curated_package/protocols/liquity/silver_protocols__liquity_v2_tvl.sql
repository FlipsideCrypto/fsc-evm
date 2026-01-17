{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'liquity', 'tvl', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Liquity V2 TVL

    Original query: https://dune.com/queries/4611479

    Tracks token balances across Liquity V2 pools:
    - WETH pools: active (0xacece9a6...), default (0x075e0c70...), stability (0xf69eb8c0...)
    - wstETH pools: active (0x2fcf4e86...), default (0x4fd6b1d4...), stability (0xcf46dab5...)
    - rETH pools: active (0x4b073907...), default (0x26b1d857...), stability (0xc4463b26...)
#}

WITH constants AS (
    SELECT '2025-01-14' AS bold_deployment_date
),

token AS (
    SELECT symbol, address
    FROM (
        VALUES
            ('BOLD', '0xb01dd87b29d187f3e3a4bf6cdaebfb97f3d9ab98'),
            ('WETH', '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'),
            ('wstETH', '0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0'),
            ('rETH', '0xae78736cd615f374d3085123a210448e74fc6393')
    ) AS token (symbol, address)
),

pool AS (
    SELECT *
    FROM (
        VALUES
            -- WETH pools
            ('0xacece9a6ff7fea9b9e1cdfeee61ca2b45cc4627b'), -- active
            ('0x075e0c707097c4c071056687b0b87cd9392c7bbd'), -- default
            ('0xf69eb8c0d95d4094c16686769460f678727393cf'), -- stability
            -- wstETH pools
            ('0x2fcf4e86594aadd744f82fd80d5da9b72ab50d7c'), -- active
            ('0x4fd6b1d48900e41710db9f219e153bb56727192b'), -- default
            ('0xcf46dab575c364a8b91bda147720ff4361f4627f'), -- stability
            -- rETH pools
            ('0x4b0739071d85444121b17b7d0ee23672825d7cff'), -- active
            ('0x26b1d8571560c7942e6dd79377721be81ae817a4'), -- default
            ('0xc4463b26be1a6064000558a84ef9b6a58abe4f7a') -- stability
    ) AS pool (address)
),

range AS (
    SELECT DISTINCT
        date
        , token.symbol AS token_symbol
        , token.address AS token_address
    FROM {{ ref('utils__date_spine') }}, token, constants
    WHERE date BETWEEN bold_deployment_date AND TO_DATE(SYSDATE())
),

xfer AS (
    SELECT
        block_timestamp AS evt_block_time
        , tx_hash
        , contract_address
        , to_address
        , from_address
        , amount AS value
        , modified_timestamp
    FROM {{ ref('core__ez_token_transfers') }}, constants
    WHERE block_timestamp >= bold_deployment_date
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}
),

balance AS (
    SELECT
        evt_block_time AS time
        , tx_hash
        , token.symbol AS token_symbol
        , value AS change
        , xfer.modified_timestamp
    FROM xfer
    JOIN token ON xfer.contract_address = token.address
    JOIN pool ON xfer.to_address = pool.address

    UNION ALL

    SELECT
        evt_block_time
        , tx_hash
        , token.symbol
        , -value
        , xfer.modified_timestamp
    FROM xfer
    JOIN token ON xfer.contract_address = token.address
    JOIN pool ON xfer.from_address = pool.address
),

daily AS (
    SELECT
        range.*
        , COALESCE(SUM(balance.change), 0) AS change
        , MAX(balance.modified_timestamp) AS modified_timestamp
    FROM range
    LEFT JOIN balance ON (
        range.date = DATE_TRUNC('day', balance.time)
        AND range.token_symbol = balance.token_symbol
    )
    GROUP BY range.date, range.token_symbol, range.token_address
),

cume AS (
    SELECT
        date
        , token_symbol
        , token_address
        , SUM(change) OVER (PARTITION BY token_symbol ORDER BY date) AS balance
        , modified_timestamp
    FROM daily
)

SELECT
    cume.date
    , 'ethereum' AS chain
    , 'Liquity' AS protocol
    , 'v2' AS version
    , token_symbol AS token
    , balance AS tvl_native
    , balance * CASE token_symbol
        WHEN 'BOLD' THEN COALESCE(price, 1) -- no BOLD price in the beginning
        ELSE price
    END AS tvl_usd
    , MAX(cume.modified_timestamp) OVER (ORDER BY cume.date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS modified_timestamp
    , SYSDATE() AS inserted_timestamp
    , '{{ invocation_id }}' AS _invocation_id
FROM cume
LEFT JOIN {{ ref('price__ez_prices_hourly') }} price ON (
    cume.token_address = price.token_address
    AND cume.date = price.hour
)
