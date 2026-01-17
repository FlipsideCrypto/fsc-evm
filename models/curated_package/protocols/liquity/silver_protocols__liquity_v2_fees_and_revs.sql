{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'fee_type'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'liquity', 'fees_and_revs', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Liquity V2 Fees and Revenues

    Original query: https://dune.com/queries/4462463/7465203

    Tracks three types of fees:
    1. SP Yield - Stability Pool yield from wstETH, rETH, WETH collateral types
    2. PIL Yield - Protocol Incentivized Liquidity yield
    3. Redemption Fees - ETH redemption fees
#}

WITH days AS (
    SELECT date AS day
    FROM {{ ref('utils__date_spine') }}
    WHERE date BETWEEN '2025-01-01' AND TO_DATE(SYSDATE())
),

eth_fee AS (
    SELECT
        block_timestamp::DATE AS date
        , tx_hash
        , utils.udf_hex_to_int(data)::NUMBER / 1e18 AS fee
        , modified_timestamp
    FROM {{ ref('core__fact_event_logs') }}
    WHERE topics[0] = LOWER('0xc7e8309b9b14e7a8561ed352b9fd8733de32417fb7b6a69f5671f79e7bb29ddd')
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}
),

add_price AS (
    SELECT
        ef.date
        , ef.tx_hash
        , ef.fee
        , p.price
        , ef.fee * p.price AS fee_usd
        , ef.modified_timestamp
    FROM eth_fee ef
    LEFT JOIN {{ ref('price__ez_prices_hourly') }} p
        ON ef.date = p.hour
        AND p.symbol = 'WETH'
),

interest_rewards AS (
    SELECT
        CASE
            WHEN to_address = '0xcf46dab575c364a8b91bda147720ff4361f4627f' THEN 'wstETH'
            WHEN to_address = '0xc4463b26be1a6064000558a84ef9b6a58abe4f7a' THEN 'rETH'
            WHEN to_address = '0xf69eb8c0d95d4094c16686769460f678727393cf' THEN 'WETH'
            WHEN to_address = '0x636deb767cd7d0f15ca4ab8ea9a9b26e98b426ac' THEN 'PIL'
        END AS collateral_type
        , DATE_TRUNC('day', block_timestamp) AS day
        , SUM(amount) AS bold_amount
        , MAX(modified_timestamp) AS modified_timestamp
    FROM {{ ref('core__ez_token_transfers') }}
    WHERE to_address IN (
        '0xcf46dab575c364a8b91bda147720ff4361f4627f',
        '0xc4463b26be1a6064000558a84ef9b6a58abe4f7a',
        '0xf69eb8c0d95d4094c16686769460f678727393cf',
        '0x636deb767cd7d0f15ca4ab8ea9a9b26e98b426ac'
    )
    AND from_address = '0x0000000000000000000000000000000000000000'
    AND contract_address = LOWER('0xb01dd87b29d187f3e3a4bf6cdaebfb97f3d9ab98')
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}
    GROUP BY 1, 2
),

all_interest AS (
    SELECT
        day
        , CASE
            WHEN collateral_type IN ('wstETH', 'rETH', 'WETH') THEN 'SP Yield'
            ELSE 'PIL Yield'
        END AS fee_type
        , SUM(bold_amount) AS fee
        , MAX(modified_timestamp) AS modified_timestamp
    FROM interest_rewards
    WHERE collateral_type IN ('wstETH', 'rETH', 'WETH', 'PIL')
    GROUP BY 1, 2

    UNION ALL

    SELECT
        date AS day
        , 'Redemption Fees' AS fee_type
        , SUM(fee_usd) AS fee
        , MAX(modified_timestamp) AS modified_timestamp
    FROM add_price
    GROUP BY 1, 2
),

get_next_day AS (
    SELECT
        *
        , SUM(fee) OVER (PARTITION BY fee_type ORDER BY day ASC) AS fee_total
        , LEAD(day, 1, CURRENT_TIMESTAMP) OVER (PARTITION BY fee_type ORDER BY day ASC) AS next_day
    FROM all_interest
),

final_data AS (
    SELECT
        b.day
        , b.fee_type
        , COALESCE(c.fee, 0) AS fee
        , c.modified_timestamp
    FROM (
        SELECT
            d.day
            , c.fee_type
            , c.fee_total
        FROM get_next_day c
        INNER JOIN days d
            ON c.day <= d.day
            AND d.day < c.next_day
    ) b
    LEFT JOIN get_next_day c
        ON b.day = c.day
        AND b.fee_type = c.fee_type
)

SELECT
    day AS date
    , 'ethereum' AS chain
    , 'Liquity' AS protocol
    , 'v2' AS version
    , 'BOLD' AS token
    , fee_type
    , fee AS revenue_native
    , fee AS revenue_usd
    , MAX(modified_timestamp) AS modified_timestamp
    , SYSDATE() AS inserted_timestamp
    , '{{ invocation_id }}' AS _invocation_id
FROM final_data
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
