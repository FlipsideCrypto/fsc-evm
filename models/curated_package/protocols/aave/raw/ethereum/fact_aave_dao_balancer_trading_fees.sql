{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Protocol-specific addresses #}
{% set balancer_pool = vars.PROTOCOL_AAVE_BALANCER_POOL %}

with
swaps as (
    select
        block_timestamp
        , decoded_log:tokenIn::string as token_address
        , decoded_log:tokenAmountIn::float * 0.001 as amount
    from {{ ref('core__ez_decoded_event_logs') }}
    where contract_address = lower('{{ balancer_pool }}') 
        and event_name = 'LOG_SWAP'
)
, swap_revenue as (
    select
        block_timestamp::date as date
        , swaps.token_address
        , coalesce(amount / pow(10, decimals), 0) as amount_nominal
        , coalesce(amount_nominal * price, 0) as amount_usd
    from swaps
    left join {{ ref('price__ez_prices_hourly') }} p
        on date_trunc(hour, block_timestamp) = hour 
        and lower(swaps.token_address) = lower(p.token_address)
)
select
    date
    , token_address
    , 'AAVE DAO' as protocol
    , 'ethereum' as chain
    , sum(coalesce(amount_nominal, 0)) as trading_fees_nominal
    , sum(coalesce(amount_usd, 0)) as trading_fees_usd
from swap_revenue 
where date < to_date(sysdate())
group by 1, 2
order by 1 desc