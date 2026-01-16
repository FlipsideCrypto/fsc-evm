{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'contract_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'convex', 'tvl', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Convex Gauge Tokens TVL

    Aggregates TVL by gauge token address with USD valuations.
    Uses Curve LP token prices for valuation.
#}

WITH lp_token_prices AS (
    SELECT
        date,
        contract_address,
        price
    FROM {{ ref('fact_curve_lp_token_prices') }}
)

SELECT
    t.date,
    t.token_address AS contract_address,
    COALESCE(t.name, ep0.symbol) AS symbol,
    SUM(t.balance_native) AS balance_native,
    SUM(t.balance_native * COALESCE(lp.price, ep0.price)) AS balance_usd,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM {{ ref('silver_protocols__convex_staked_tvl_by_token') }} t
LEFT JOIN {{ ref('dim_curve_pools') }} cp
    ON LOWER(cp.token) = LOWER(t.token_address)
LEFT JOIN {{ ref('price__ez_prices_hourly') }} ep0
    ON ep0.hour = t.date
    AND LOWER(ep0.token_address) = LOWER(cp.coin_0)
LEFT JOIN lp_token_prices lp
    ON t.date = lp.date
    AND LOWER(lp.contract_address) = LOWER(t.token_address)
WHERE 1 = 1
    AND NOT (ep0.price IS NULL AND lp.price IS NULL)
    AND t.date < TO_DATE(SYSDATE())
{% if is_incremental() %}
    AND t.modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
        FROM {{ this }}
    )
    AND t.modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
{% endif %}
GROUP BY 1, 2, 3
