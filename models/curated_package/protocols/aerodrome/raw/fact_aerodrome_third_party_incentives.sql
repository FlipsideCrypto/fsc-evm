{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- This is a model abstracting bribes for Aerodrome.
-- Bribes are third_party_incentives that are paid to veAERO holders to incentivize them to vote to allocate
-- emissions to the third party protocol's token pools and drive liquidity for their token.

WITH claim_rewards_events AS (
    SELECT 
        DATE(block_timestamp) AS date
        , DATE_TRUNC(HOUR, block_timestamp) AS hour
        , block_timestamp
        , tx_hash 
        , event_index
        , decoded_log:"amount"::INT AS amount
        , decoded_log:"from"::VARCHAR AS from_address
        , decoded_log:"reward"::VARCHAR AS reward_token
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE 1=1
        AND topic_0 = '0x9aa05b3d70a9e3e2f004f039648839560576334fb45c81f91b6db03ad9e2efc9' -- this is the topic0 for the ClaimRewards event
        AND origin_function_signature = '0x7715ee75' -- this is the function call signature for the Claim Bribes function
        {% if is_incremental() %}
            AND block_timestamp > (SELECT MAX(this.block_timestamp) FROM {{ this }} as this)
        {% endif %}
)

SELECT 
    cre.date
    , cre.hour
    , cre.block_timestamp
    , cre.tx_hash
    , cre.event_index
    , cre.amount
    , cre.from_address
    , cre.reward_token
    , p.symbol
    , (cre.amount / POW(10, p.decimals)) * p.price AS amount_usd
FROM claim_rewards_events cre
LEFT JOIN {{ ref('price__ez_prices_hourly') }} p
    ON cre.hour = p.hour
    AND cre.reward_token = p.token_address