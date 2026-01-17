{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'liquity', 'fees_and_revs', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Liquity V1 Fees and Revenues

    Tracks fees from two sources:
    1. LUSD borrowing fees from the BorrowerOperations contract
    2. ETH redemption fees from the TroveManager contract
#}

WITH lusd AS (
    SELECT
        block_timestamp::DATE AS date
        , 'LUSD' AS token
        , decoded_log:_LUSDFee / 1e18 AS revenue_native
        , decoded_log:_LUSDFee / 1e18 AS revenue_usd
        , modified_timestamp
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE contract_address = LOWER('0x24179cd81c9e782a4096035f7ec97fb8b783e007')
    AND event_name = 'LUSDBorrowingFeePaid'
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}
),

eth AS (
    SELECT
        e.block_timestamp::DATE AS date
        , 'ETH' AS token
        , e.decoded_log:_ETHFee / 1e18 AS revenue_native
        , e.decoded_log:_ETHFee / 1e18 * p.price AS revenue_usd
        , e.modified_timestamp
    FROM {{ ref('core__ez_decoded_event_logs') }} e
    LEFT JOIN {{ ref('price__ez_prices_hourly') }} p
        ON p.hour = e.block_timestamp::DATE
        AND p.is_native
    WHERE e.contract_address = LOWER('0xa39739ef8b0231dbfa0dcda07d7e29faabcf4bb2')
    AND e.event_name = 'Redemption'
    {% if is_incremental() %}
    AND e.modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
        FROM {{ this }}
    )
    AND e.modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}
),

combined AS (
    SELECT date, token, revenue_native, revenue_usd, modified_timestamp FROM lusd
    UNION ALL
    SELECT date, token, revenue_native, revenue_usd, modified_timestamp FROM eth
)

SELECT
    date
    , 'ethereum' AS chain
    , 'Liquity' AS protocol
    , 'v1' AS version
    , token
    , SUM(revenue_native) AS revenue_native
    , SUM(revenue_usd) AS revenue_usd
    , MAX(modified_timestamp) AS modified_timestamp
    , SYSDATE() AS inserted_timestamp
    , '{{ invocation_id }}' AS _invocation_id
FROM combined
GROUP BY 1, 2, 3, 4, 5
