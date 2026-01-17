{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'chainlink', 'staking', 'ethereum', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Chainlink Staking Rewards on Ethereum

    Tracks staking rewards from both V1 and V2 staking contracts.
    V1 uses RewardClaimed event, V2 uses RewardsVault transfers.
#}

WITH v2_rewards_raw AS (
    SELECT
        t.block_timestamp::date AS date
        , SUM(amount) AS link
        , MAX(t.modified_timestamp) AS modified_timestamp
    FROM {{ ref('core__ez_token_transfers') }} t
    LEFT JOIN {{ ref('core__ez_decoded_event_logs') }} l ON t.tx_hash = l.tx_hash
    WHERE LOWER(t.from_address) = LOWER('0x996913c8c08472f584ab8834e925b06D0eb1D813')
        AND l.topics[0]::string = '0x106f923f993c2149d49b4255ff723acafa1f2d94393f561d3eda32ae348f7241'
        AND LOWER(l.contract_address) = LOWER('0x996913c8c08472f584ab8834e925b06D0eb1D813')
    {% if is_incremental() %}
    AND t.modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
        FROM {{ this }}
    )
    AND t.modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}
    GROUP BY 1
)
, v1_rewards_raw AS (
    SELECT
        block_timestamp
        , tx_hash
        , decoded_log:"staker"::string AS staker
        , decoded_log:"principal"::number AS principal
        , decoded_log:"baseReward"::number AS base_reward
        , decoded_log:"delegationReward"::number AS delegate_reward
        , modified_timestamp
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE topics[0]::string = '0x667838b33bdc898470de09e0e746990f2adc11b965b7fe6828e502ebc39e0434'
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}
)
, all_rewards AS (
    SELECT
        block_timestamp::date AS date
        , SUM(base_reward + delegate_reward)/1e18 AS rewards
        , MAX(modified_timestamp) AS modified_timestamp
    FROM v1_rewards_raw
    GROUP BY 1

    UNION ALL

    SELECT
        date
        , SUM(link) AS rewards
        , MAX(modified_timestamp) AS modified_timestamp
    FROM v2_rewards_raw
    GROUP BY 1
)
, prices AS (
    {{ get_coingecko_price_with_latest('chainlink') }}
)
, daily_rewards AS (
    SELECT
        all_rewards.date
        , SUM(rewards) AS staking_rewards_native
        , MAX(all_rewards.modified_timestamp) AS modified_timestamp
    FROM all_rewards
    GROUP BY 1
)

SELECT
    daily_rewards.date
    , 'ethereum' AS chain
    , staking_rewards_native
    , staking_rewards_native * prices.price AS staking_rewards
    , daily_rewards.modified_timestamp
    , SYSDATE() AS inserted_timestamp
    , '{{ invocation_id }}' AS _invocation_id
FROM daily_rewards
LEFT JOIN prices USING(date)
