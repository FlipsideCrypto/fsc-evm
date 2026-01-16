{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['tx_hash', 'token_address', 'claimer'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'eigenlayer', 'rewards_claimed', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Eigenlayer AVS Rewards Claimed

    Tracks rewards claimed through the Eigenlayer Rewards Coordinator contract.
    Includes USD valuations based on token prices at the time of claim.
    Excludes EIGEN token claims.
#}

WITH AVSRewardsClaimedEvents AS (
    SELECT
        date_trunc('day', block_timestamp) as date,
        block_timestamp,
        tx_hash,
        decoded_log,
        decoded_log:claimedAmount::FLOAT as amount,
        decoded_log:claimer::STRING as claimer,
        decoded_log:recipient::STRING as recipient,
        decoded_log:token::STRING as token_address
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE contract_address = lower('0x7750d328b314EfFa365A0402CcfD489B80B0adda') --Eigenlayer Rewards Coordinator
    AND event_name = 'RewardsClaimed'
    {% if is_incremental() %}
    AND block_timestamp >= (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),

token_info AS (
    SELECT
        hour,
        token_address,
        price,
        symbol,
        decimals
    FROM {{ ref('price__ez_prices_hourly') }}
),

AVSRewardsClaimedEvents_USD AS (
    SELECT
        rce.*,
        t.decimals,
        rce.amount / pow(10,t.decimals) as amount_adj,
        t.symbol as token_symbol,
        t.price as token_price,
        (rce.amount / POW(10, t.decimals)) * t.price AS amount_usd
    FROM AVSRewardsClaimedEvents rce
    LEFT JOIN token_info t
        ON lower(t.token_address) = lower(rce.token_address)
        AND t.hour = rce.date
)

SELECT
    date,
    block_timestamp,
    tx_hash,
    decoded_log,
    amount,
    claimer,
    recipient,
    token_address,
    decimals,
    amount_adj,
    token_symbol,
    token_price,
    amount_usd,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM AVSRewardsClaimedEvents_USD
WHERE token_symbol != 'EIGEN'
