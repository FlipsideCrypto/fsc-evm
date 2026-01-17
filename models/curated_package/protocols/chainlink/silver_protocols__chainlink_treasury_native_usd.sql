{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'chainlink', 'treasury', 'ethereum', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Chainlink Treasury Native USD

    Calculates the cumulative LINK balance in Chainlink non-circulating supply addresses.
    Uses flows (in/out) to track treasury balance over time.
#}

WITH base AS (
    SELECT
        to_address
        , from_address
        , TO_DATE(block_timestamp) AS date
        , amount_precise
        , MIN(TO_DATE(block_timestamp)) OVER() AS min_date
        , MAX(modified_timestamp) OVER (PARTITION BY TO_DATE(block_timestamp)) AS modified_timestamp
    FROM {{ ref('core__ez_token_transfers') }}
    WHERE LOWER(contract_address) = LOWER('0x514910771AF9Ca656af840dff83E8264EcF986CA')
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}
)
, date_range AS (
    SELECT *
    FROM (
        SELECT
            min_date + SEQ4() AS date
        FROM base
    )
    WHERE date <= TO_DATE(SYSDATE())
)
, address_cte AS (
    {{ chainlink_non_circulating_supply_addresses() }}
)
, flows AS (
    SELECT
        date
        , SUM(CASE WHEN to_address IN (SELECT address FROM address_cte) THEN amount_precise ELSE 0 END) AS amount_in
        , SUM(CASE WHEN from_address IN (SELECT address FROM address_cte) THEN amount_precise ELSE 0 END) AS amount_out
        , MAX(modified_timestamp) AS modified_timestamp
    FROM base
    GROUP BY 1
)
, prices AS (
    SELECT
        DATE(hour) AS date
        , AVG(price) AS price
    FROM {{ ref('price__ez_prices_hourly') }}
    WHERE LOWER(token_address) = LOWER('0x514910771AF9Ca656af840dff83E8264EcF986CA')
    GROUP BY 1
)

SELECT
    dr.date AS date
    , SUM(COALESCE(f.amount_in, 0) - COALESCE(f.amount_out, 0)) OVER (ORDER BY dr.date) AS treasury_link
    , treasury_link * p.price AS treasury_usd
    , COALESCE(f.modified_timestamp, SYSDATE()) AS modified_timestamp
    , SYSDATE() AS inserted_timestamp
    , '{{ invocation_id }}' AS _invocation_id
FROM date_range dr
LEFT JOIN flows f ON f.date = dr.date
LEFT JOIN prices p ON p.date = dr.date
