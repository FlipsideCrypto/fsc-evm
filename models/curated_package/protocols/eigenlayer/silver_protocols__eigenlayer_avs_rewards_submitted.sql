{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['tx_hash', 'token_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'eigenlayer', 'rewards_submitted', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Eigenlayer AVS Rewards Submitted

    Tracks rewards submissions to the Eigenlayer Rewards Coordinator contract.
    Captures both RewardsSubmissionForAllEarnersCreated and AVSRewardsSubmissionCreated events.
    Includes USD valuations based on token prices at the time of submission.
#}

WITH AVSRewardsSubmittedEvents AS (
    SELECT
        date_trunc('day', block_timestamp) as date,
        block_timestamp,
        tx_hash,
        event_name,
        decoded_log,
        decoded_log:rewardsSubmission[1]::STRING as token_address,
        decoded_log:rewardsSubmission[2]::STRING as amount
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE contract_address = lower('0x7750d328b314EfFa365A0402CcfD489B80B0adda') --Eigenlayer Rewards Coordinator
    AND event_name in ('RewardsSubmissionForAllEarnersCreated', 'AVSRewardsSubmissionCreated')
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

AVSRewardsSubmittedEvents_USD AS (
    SELECT
        rse.*,
        t.decimals,
        rse.amount / pow(10,t.decimals) as amount_adj,
        t.symbol as token_symbol,
        t.price as token_price,
        (rse.amount / POW(10, t.decimals)) * t.price AS amount_usd
    FROM AVSRewardsSubmittedEvents rse
    LEFT JOIN token_info t
        ON lower(t.token_address) = lower(rse.token_address)
        AND t.hour = rse.date
)

SELECT
    date,
    block_timestamp,
    tx_hash,
    event_name,
    decoded_log,
    token_address,
    amount,
    decimals,
    amount_adj,
    token_symbol,
    token_price,
    amount_usd,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM AVSRewardsSubmittedEvents_USD
