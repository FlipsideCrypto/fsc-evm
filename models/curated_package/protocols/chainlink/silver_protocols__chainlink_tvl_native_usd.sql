{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'chainlink', 'tvl', 'ethereum', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Chainlink TVL Native USD

    Tracks total value locked in key Chainlink staking contracts:
    - 0xBc10f2E862ED4502144c7d632a3459F49DFCDB5e (Staking V1)
    - 0xA1d76A7cA72128541E9FCAcafBdA3a92EF94fDc5 (Staking V2)
    - 0x3feB1e09b4bb0E7f0387CeE092a52e85797ab889 (Community Staking)
#}

WITH filtered_balances AS (
    SELECT
        DATE(block_timestamp) AS date
        , address
        , MAX_BY(balance_token / 1e18, block_timestamp) AS balance_token
        , MAX(modified_timestamp) AS modified_timestamp
    FROM {{ ref('core__fact_token_balances') }}
    WHERE contract_address = '0x514910771af9ca656af840dff83e8264ecf986ca'
        AND LOWER(address) IN (
            LOWER('0xBc10f2E862ED4502144c7d632a3459F49DFCDB5e'),
            LOWER('0xA1d76A7cA72128541E9FCAcafBdA3a92EF94fDc5'),
            LOWER('0x3feB1e09b4bb0E7f0387CeE092a52e85797ab889')
        )
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}
    GROUP BY 1, 2
)
, unique_dates AS (
    SELECT DISTINCT DATE(block_timestamp) AS date
    FROM {{ ref('core__fact_token_balances') }}
    WHERE block_timestamp > '2022-12-06'
)
, addresses AS (
    SELECT DISTINCT address
    FROM filtered_balances
)
, all_combinations AS (
    SELECT
        ud.date
        , a.address
    FROM unique_dates ud
    CROSS JOIN addresses a
)
, joined_balances AS (
    SELECT
        ac.date
        , ac.address
        , fb.balance_token
        , fb.modified_timestamp
    FROM all_combinations ac
    LEFT JOIN filtered_balances fb
        ON ac.date = fb.date
        AND ac.address = fb.address
)
, prices AS (
    SELECT
        DATE(hour) AS date
        , AVG(price) AS price
    FROM {{ ref('price__ez_prices_hourly') }}
    WHERE LOWER(token_address) = LOWER('0x514910771AF9Ca656af840dff83E8264EcF986CA')
    GROUP BY 1
)
, filled_balances AS (
    SELECT
        j.date
        , address
        , COALESCE(
            balance_token,
            LAST_VALUE(balance_token IGNORE NULLS) OVER (
                PARTITION BY address ORDER BY j.date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS balance_token
        , COALESCE(
            balance_token,
            LAST_VALUE(balance_token IGNORE NULLS) OVER (
                PARTITION BY address ORDER BY j.date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) * p.price AS balance_usd
        , COALESCE(j.modified_timestamp, SYSDATE()) AS modified_timestamp
    FROM joined_balances j
    LEFT JOIN prices p ON p.date = j.date
)

SELECT
    date
    , SUM(balance_usd) AS balance_usd
    , SUM(balance_token) AS balance_link
    , MAX(modified_timestamp) AS modified_timestamp
    , SYSDATE() AS inserted_timestamp
    , '{{ invocation_id }}' AS _invocation_id
FROM filled_balances
GROUP BY date
