{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['tx_hash', 'event_index'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_protocols', 'aerodrome', 'third_party_incentives', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Aerodrome Third Party Incentives (Bribes)

    Bribes are third party incentives paid to veAERO holders to incentivize them
    to vote to allocate emissions to the third party protocol's token pools
    and drive liquidity for their token.
#}

WITH claim_rewards_events AS (
    SELECT
        DATE(block_timestamp) AS date
        , DATE_TRUNC(HOUR, block_timestamp) AS hour
        , block_timestamp
        , block_number
        , tx_hash
        , event_index
        , decoded_log:"amount"::INT AS amount
        , decoded_log:"from"::VARCHAR AS from_address
        , decoded_log:"reward"::VARCHAR AS reward_token
        , modified_timestamp
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE 1 = 1
        AND topic_0 = '0x9aa05b3d70a9e3e2f004f039648839560576334fb45c81f91b6db03ad9e2efc9' -- ClaimRewards event
        AND origin_function_signature = '0x7715ee75' -- Claim Bribes function
    {% if is_incremental() %}
        AND modified_timestamp >= (
            SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
            FROM {{ this }}
        )
        AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}
)

SELECT
    cre.date
    , cre.hour
    , cre.block_timestamp
    , cre.block_number
    , cre.tx_hash
    , cre.event_index
    , cre.amount
    , cre.from_address
    , cre.reward_token
    , p.symbol
    , (cre.amount / POW(10, p.decimals)) * p.price AS amount_usd
    , cre.modified_timestamp
    , SYSDATE() AS inserted_timestamp
    , '{{ invocation_id }}' AS _invocation_id
FROM claim_rewards_events cre
LEFT JOIN {{ ref('price__ez_prices_hourly') }} p
    ON cre.hour = p.hour
    AND cre.reward_token = p.token_address
