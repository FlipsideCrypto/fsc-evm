{{ config(
    materialized = 'table',
    tags = ['silver_protocols', 'lido', 'fee_split', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Lido Fee Split History

    Lido takes a 10% fee on staking rewards, distributed as follows:
    - Treasury/Insurance: 5%
    - Node Operators: 5%
    - Stakers receive: 90%

    This model provides the historical fee percentages by date range.
    Fee structure has remained constant since launch (Dec 2020).
#}

WITH fee_periods AS (
    SELECT
        '2020-12-17'::date AS start_date
        , CURRENT_DATE() AS end_date
        , 0.05 AS treasury_fee_pct
        , 0.00 AS insurance_fee_pct
        , 0.05 AS operators_fee_pct
),
date_spine AS (
    SELECT DATEADD('day', seq4(), '2020-12-17'::date) AS date
    FROM TABLE(GENERATOR(ROWCOUNT => 10000))
    WHERE date <= CURRENT_DATE()
)

SELECT
    d.date
    , fp.treasury_fee_pct
    , fp.insurance_fee_pct
    , fp.operators_fee_pct
    , fp.treasury_fee_pct + fp.insurance_fee_pct + fp.operators_fee_pct AS total_fee_pct
FROM date_spine d
CROSS JOIN fee_periods fp
WHERE d.date >= fp.start_date
  AND d.date <= fp.end_date
