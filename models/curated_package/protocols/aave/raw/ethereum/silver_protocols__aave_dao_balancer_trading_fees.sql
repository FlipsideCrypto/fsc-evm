{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Protocol-specific addresses #}
{% set balancer_pool = vars.PROTOCOL_AAVE_BALANCER_POOL %}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'aave', 'dao_balancer_trading_fees', 'curated']
) }}

WITH
swaps AS (
    SELECT
        block_number
        , block_timestamp
        , decoded_log:tokenIn::string AS token_address
        , decoded_log:tokenAmountIn::float * 0.001 AS amount
        , modified_timestamp
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE contract_address = LOWER('{{ balancer_pool }}')
        AND event_name = 'LOG_SWAP'
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}
)
, swap_revenue AS (
    SELECT
        block_number
        , block_timestamp::date AS date
        , swaps.token_address
        , COALESCE(amount / POW(10, decimals), 0) AS amount_nominal
        , COALESCE(amount_nominal * price, 0) AS amount_usd
        , swaps.modified_timestamp
    FROM swaps
    LEFT JOIN {{ ref('price__ez_prices_hourly') }} p
        ON date_trunc(hour, block_timestamp) = hour
        AND LOWER(swaps.token_address) = LOWER(p.token_address)
)
SELECT
    date
    , token_address
    , 'AAVE DAO' AS protocol
    , 'ethereum' AS chain
    , SUM(COALESCE(amount_nominal, 0)) AS trading_fees_nominal
    , SUM(COALESCE(amount_usd, 0)) AS trading_fees_usd
    , MAX(block_number) AS block_number
    , MAX(modified_timestamp) AS modified_timestamp
FROM swap_revenue
WHERE date < TO_DATE(SYSDATE())
GROUP BY 1, 2
