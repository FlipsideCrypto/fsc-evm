{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

With AVSRewardsClaimedEvents AS (
    select
        date_trunc('day', block_timestamp) as date,
        block_timestamp,
        tx_hash,
        decoded_log,
        decoded_log:claimedAmount::FLOAT as amount,
        decoded_log:claimer::STRING as claimer,
        decoded_log:recipient::STRING as recipient,
        decoded_log:token::STRING as token_address,
    from {{ ref('core__ez_decoded_event_logs') }}
    where contract_address = lower('0x7750d328b314EfFa365A0402CcfD489B80B0adda') --Eigenlayer Rewards Coordinator
    and event_name = 'RewardsClaimed'
), token_info AS (
    SELECT
        hour,
        token_address,
        price,
        symbol,
        decimals
    FROM {{ ref('price__ez_prices_hourly') }}
), AVSRewardsClaimedEvents_USD AS (
    select
        rce.*,
        t.decimals,
        rce.amount / pow(10,t.decimals) as amount_aduj,
        t.symbol as token_symbol,
        t.price as token_price,
        (rce.amount / POW(10, t.decimals)) * t.price AS amount_usd
    from AVSRewardsClaimedEvents rce
    LEFT JOIN token_info t 
        ON lower(t.token_address) = lower(rce.token_address) 
        and t.hour = rce.date
) select * from AVSRewardsClaimedEvents_USD WHERE token_symbol != 'EIGEN'