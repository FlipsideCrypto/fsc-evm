{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'liquity', 'tvl', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Liquity V1 TVL

    Tracks ETH collateral locked in the Liquity V1 ActivePool contract:
    0xdf9eb223bafbe5c5271415c75aecd68c21fe3d7f
#}

WITH traces AS (
    SELECT
        block_timestamp::DATE AS date
        , SUM(
            CASE
                WHEN to_address = LOWER('0xdf9eb223bafbe5c5271415c75aecd68c21fe3d7f') THEN value
                ELSE -(value)
            END
        ) AS val
        , MAX(modified_timestamp) AS modified_timestamp
    FROM {{ ref('core__fact_traces') }}
    WHERE tx_succeeded = TRUE
    AND (
        to_address = LOWER('0xdf9eb223bafbe5c5271415c75aecd68c21fe3d7f')
        OR from_address = LOWER('0xdf9eb223bafbe5c5271415c75aecd68c21fe3d7f')
    )
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}
    GROUP BY 1
),

date_spine AS (
    SELECT date
    FROM {{ ref('dim_date_spine') }}
    WHERE date BETWEEN (SELECT MIN(date) FROM traces) AND TO_DATE(SYSDATE())
),

left_join AS (
    SELECT
        ds.date
        , t.val
        , t.modified_timestamp
    FROM date_spine ds
    LEFT JOIN traces t ON ds.date = t.date
),

summed AS (
    SELECT
        date
        , SUM(val) OVER (ORDER BY date) AS val
        , modified_timestamp
    FROM left_join
),

filled AS (
    SELECT
        date
        , COALESCE(val, LAST_VALUE(val IGNORE NULLS) OVER (ORDER BY date)) AS val
        , modified_timestamp
    FROM summed
)

SELECT
    t.date
    , 'ethereum' AS chain
    , 'Liquity' AS protocol
    , 'v1' AS version
    , 'ETH' AS token
    , t.val AS tvl_native
    , t.val * p.price AS tvl_usd
    , MAX(t.modified_timestamp) OVER (ORDER BY t.date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS modified_timestamp
    , SYSDATE() AS inserted_timestamp
    , '{{ invocation_id }}' AS _invocation_id
FROM filled t
LEFT JOIN {{ ref('price__ez_prices_hourly') }} p
    ON p.hour = t.date
    AND p.is_native
