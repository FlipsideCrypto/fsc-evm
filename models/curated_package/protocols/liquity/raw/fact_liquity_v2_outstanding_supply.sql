{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

with daily_change as (
    SELECT
        block_timestamp::date as date,
        SUM(CASE WHEN from_address = '0x0000000000000000000000000000000000000000'
            THEN amount
            WHEN to_address  = '0x0000000000000000000000000000000000000000'
            THEN -1 * amount
        END) as net
    FROM
        {{ ref('core__ez_token_transfers') }}
    WHERE 1=1
        AND contract_address = lower('0xb01dd87b29d187f3e3a4bf6cdaebfb97f3d9ab98')
    GROUP BY 1
)
, sparse as (
    SELECT
        ds.date,
        net
    FROM {{ ref('dim_date_spine') }} ds
    LEFT JOIN daily_change c using(date)
    WHERE date between (SELECT min(date) FROM daily_change) AND to_date(sysdate())
)
SELECT
    date,
    'ethereum' as chain,
    'BOLD' as token,
    SUM(net) OVER (ORDER BY date ASC) as outstanding_supply
FROM
    sparse