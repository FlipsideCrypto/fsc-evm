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
    Convex CVX Tokens TVL

    Aggregates cvxCRV and cvxFXS token balances with USD valuations.
    Combines data from fact_convex_cvxcrv_balance and fact_convex_cvxfxs_balance.
#}

WITH agg AS (
    SELECT
        date,
        contract_address,
        balance,
        modified_timestamp
    FROM {{ ref('fact_convex_cvxcrv_balance') }}
    {% if is_incremental() %}
    WHERE modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}

    UNION ALL

    SELECT
        date,
        contract_address,
        balance,
        modified_timestamp
    FROM {{ ref('fact_convex_cvxfxs_balance') }}
    {% if is_incremental() %}
    WHERE modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}
)

SELECT
    agg.date,
    agg.contract_address,
    agg.balance,
    CASE
        WHEN LOWER(agg.contract_address) = LOWER('0x5f3b5DfEb7B28CDbD7FAba78963EE202a494e2A2') THEN 'cvxCRV'
        ELSE 'cvxFXS'
    END AS symbol,
    CASE
        WHEN LOWER(agg.contract_address) = LOWER('0x5f3b5DfEb7B28CDbD7FAba78963EE202a494e2A2') THEN crv.price
        ELSE fxs.price
    END AS price_adj,
    agg.balance AS balance_native,
    agg.balance * CASE
        WHEN LOWER(agg.contract_address) = LOWER('0x5f3b5DfEb7B28CDbD7FAba78963EE202a494e2A2') THEN crv.price
        ELSE fxs.price
    END AS balance_usd,
    agg.modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM agg
LEFT JOIN {{ ref('price__ez_prices_hourly') }} crv
    ON crv.hour = agg.date
    AND crv.token_address = LOWER('0xD533a949740bb3306d119CC777fa900bA034cd52')
LEFT JOIN {{ ref('price__ez_prices_hourly') }} fxs
    ON fxs.hour = agg.date
    AND fxs.token_address = LOWER('0x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0')
