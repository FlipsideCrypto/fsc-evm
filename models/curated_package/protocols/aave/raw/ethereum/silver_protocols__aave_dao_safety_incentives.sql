{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Protocol-specific addresses #}
{% set stkaave = vars.PROTOCOL_AAVE_SAFETY_MODULE_STKAAVE %}
{% set aave_token = vars.PROTOCOL_AAVE_TOKEN_ADDRESS %}

with
    logs as (
        select
            block_timestamp
            , decoded_log:amount::float / 1E18 as amount_nominal
        from {{ ref('core__ez_decoded_event_logs') }}
        where contract_address = lower('{{ stkaave }}')
            and event_name = 'RewardsClaimed'
    )
    , prices as ({{get_coingecko_price_with_latest('aave')}})
    , priced_logs as (
        select
            block_timestamp::date as date
            , '{{ aave_token }}' as token_address
            , amount_nominal
            , amount_nominal * price as amount_usd
        from logs
        left join prices on block_timestamp::date = date
    )
select
    date
    , token_address
    , 'AAVE DAO' as protocol
    , 'ethereum' as chain
    , sum(coalesce(amount_nominal, 0)) as amount_nominal
    , sum(coalesce(amount_usd, 0)) as amount_usd
from priced_logs
group by 1, 2