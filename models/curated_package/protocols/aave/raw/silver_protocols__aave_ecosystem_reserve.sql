{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Protocol-specific addresses #}
{% set aave_token = vars.PROTOCOL_AAVE_TOKEN_ADDRESS %}
{% set ecosystem_reserve = vars.PROTOCOL_AAVE_ECOSYSTEM_RESERVE %}

WITH
base AS (
    select
        to_address,
        from_address,
        block_timestamp::date as date,
        amount_precise,
        min(block_timestamp::date) OVER() as min_date
    FROM {{ ref('core__ez_token_transfers') }}
    where lower(contract_address) = lower('{{ aave_token }}')
)
,  date_range AS (
    SELECT *
        FROM (
            SELECT
                min_date + SEQ4() AS date
            FROM base
        )
    WHERE date <= TO_DATE(SYSDATE())
)
, flows as (
    SELECT
        date,
        SUM(CASE WHEN to_address = lower('{{ ecosystem_reserve }}') THEN amount_precise ELSE 0 END) AS amount_in,
        SUM(CASE WHEN from_address = lower('{{ ecosystem_reserve }}') THEN amount_precise ELSE 0 END) AS amount_out
    FROM base
    GROUP BY 1
    ORDER BY 1 DESC
)
, prices as ({{get_coingecko_price_with_latest('aave')}})

SELECT
    dr.date AS date
    , 'ethereum' as chain
    , '{{ aave_token }}' as token_address
    , SUM(COALESCE(f.amount_in, 0) - COALESCE(f.amount_out, 0)) OVER (ORDER BY dr.date) as amount_nominal
    , amount_nominal * p.price as amount_usd
FROM date_range dr
LEFT JOIN flows f
    ON f.date = dr.date
LEFT JOIN prices p on p.date = dr.date
ORDER BY date DESC