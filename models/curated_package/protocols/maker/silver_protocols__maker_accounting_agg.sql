{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['period', 'account_id', 'token'],
    cluster_by = ['period'],
    tags = ['silver_protocols', 'maker', 'accounting', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH chart_of_accounts AS (
    SELECT CAST(code AS VARCHAR) AS account_id FROM {{ ref('dim_chart_of_accounts') }}
),

periods AS (
    SELECT
        DISTINCT(DATE(hour)) AS date
    FROM {{ ref('price__ez_prices_hourly') }}
    WHERE symbol = 'MKR'
    {% if is_incremental() %}
    AND hour >= DATEADD('day', -{{ vars.CURATED_LOOKBACK_DAYS }}, CURRENT_DATE())
    {% endif %}
),

accounting AS (
    SELECT
        DATE_TRUNC('day', acc.ts) AS period,
        CAST(acc.code AS VARCHAR) AS account_id,
        acc.dai_value AS usd_value,
        acc.value AS token_value,
        token
    FROM {{ ref('fact_final') }} acc
    {% if is_incremental() %}
    WHERE acc.ts >= DATEADD('day', -{{ vars.CURATED_LOOKBACK_DAYS }}, CURRENT_DATE())
    {% endif %}
),

accounting_agg AS (
    SELECT
        DATE_TRUNC('day', period) AS period,
        account_id,
        token,
        SUM(COALESCE(token_value, 0)) AS token_sum_value,
        SUM(COALESCE(usd_value, 0)) AS usd_sum_value
    FROM accounting
    GROUP BY 1, 2, 3
),

accounting_liq AS (
    SELECT DISTINCT
        period,
        token,
        SUM(COALESCE(token_sum_value, 0)) OVER (PARTITION BY DATE_TRUNC('day', period)) AS token_liq_cum,
        SUM(COALESCE(usd_sum_value, 0)) OVER (PARTITION BY DATE_TRUNC('day', period)) AS usd_liq_cum
    FROM accounting_agg
    WHERE account_id IN (
        '31210',
        '31620'
    )
)

SELECT
    a.period,
    a.account_id,
    a.token,
    CASE
        WHEN account_id = '31210' THEN IFF(usd_liq_cum > 0, usd_liq_cum, 0)
        WHEN account_id = '31620' THEN IFF(usd_liq_cum > 0, 0, usd_liq_cum)
        ELSE COALESCE(usd_sum_value, 0)
    END AS sum_value,
    CASE
        WHEN account_id = '31210' THEN IFF(token_liq_cum > 0, token_liq_cum, 0)
        WHEN account_id = '31620' THEN IFF(token_liq_cum > 0, 0, token_liq_cum)
        ELSE COALESCE(token_sum_value, 0)
    END AS sum_value_token,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM accounting_agg a
LEFT JOIN accounting_liq l
    ON a.period = l.period
    AND a.token = l.token
