{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

with eth_raw as(
    select
        block_timestamp,
        CASE
            WHEN lower(FROM_ADDRESS) = lower('0x0000000000000000000000000000000000000000') THEN AMOUNT
            WHEN lower(TO_ADDRESS) = lower('0x0000000000000000000000000000000000000000') THEN - AMOUNT
        END AS amount
    from
        {{ ref('core__ez_token_transfers') }}
    where
        lower(contract_address) = lower('0x6B175474E89094C44Da98b954EedeAC495271d0F')
        and (
            lower(FROM_ADDRESS) = lower('0x0000000000000000000000000000000000000000')
            or lower(TO_ADDRESS) = lower('0x0000000000000000000000000000000000000000')
        )
),
daily_amounts AS (
    SELECT
        date(block_timestamp) as date,
        SUM(amount) as daily_amount
    FROM eth_raw
    GROUP BY date(block_timestamp)
)
SELECT
    date,
    SUM(daily_amount) OVER (ORDER BY date) as dai_supply,
    'Ethereum' as chain
FROM daily_amounts
ORDER BY date DESC