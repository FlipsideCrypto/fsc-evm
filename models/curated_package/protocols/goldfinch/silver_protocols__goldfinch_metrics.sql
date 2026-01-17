{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'goldfinch', 'metrics', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Goldfinch Metrics

    Daily metrics tracking TVL, deposits, withdrawals, interest, and other
    key protocol statistics for Goldfinch.
#}

WITH dates AS (
    SELECT
        DISTINCT(DATE(hour)) AS date
    FROM {{ ref('price__ez_prices_hourly') }}
    WHERE date > '2020-01-01'
    {% if is_incremental() %}
    AND DATE(hour) >= (
        SELECT MAX(date) - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
        FROM {{ this }}
    )
    {% endif %}
),

events_daily AS (
    SELECT
        COALESCE(e.block_timestamp::DATE, d.date) AS dt,
        e.addr,
        CASE WHEN e.tx_type = 'deposit' THEN amount ELSE 0 END AS deposit_amt,
        CASE WHEN e.tx_type = 'withdraw' THEN amount ELSE 0 END AS withdrawal_amt,
        CASE WHEN e.tx_type = 'drawdown' THEN amount ELSE 0 END AS drawdown_amt,
        CASE WHEN e.tx_type = 'allocation' THEN amount ELSE 0 END AS allocation_amt,
        CASE WHEN e.tx_type = 'interest_paid' THEN amount ELSE 0 END AS interest_amt,
        CASE WHEN e.tx_type = 'interest_received' THEN amount ELSE 0 END AS interest_rec_amt,
        CASE WHEN e.tx_type = 'principal_paid' THEN amount ELSE 0 END AS principal_amt,
        CASE WHEN e.tx_type = 'writedown' THEN amount ELSE 0 END AS writedown_amt,
        CASE WHEN e.tx_type = 'revenue' THEN amount ELSE 0 END AS revenue_amt,
        CASE WHEN e.tx_type = 'withdrawal_revenue' THEN amount ELSE 0 END AS withdrawal_revenue_amt,
        ROW_NUMBER() OVER (PARTITION BY d.date ORDER BY e.block_timestamp DESC) AS row_number,
        e.tx_type
    FROM dates AS d
    LEFT JOIN {{ ref('silver_protocols__goldfinch_combined_events') }} AS e
        ON DATE(e.block_timestamp) = d.date
    WHERE COALESCE(e.addr, '') != '0xfd6ff39da508d281c2d255e9bbbfab34b6be60c3'
),

cumulative_calculations AS (
    SELECT
        *,
        SUM(deposit_amt) OVER (ORDER BY dt) AS deposit_cum,
        SUM(withdrawal_amt) OVER (ORDER BY dt) AS withdrawal_cum,
        SUM(drawdown_amt) OVER (ORDER BY dt) AS drawdown_cum,
        SUM(allocation_amt) OVER (ORDER BY dt) AS allocation_cum,
        SUM(interest_amt) OVER (ORDER BY dt) AS interest_cum,
        SUM(interest_rec_amt) OVER (ORDER BY dt) AS interest_rec_cum,
        SUM(principal_amt) OVER (ORDER BY dt) AS principal_cum,
        SUM(writedown_amt) OVER (ORDER BY dt) AS writedown_cum,
        SUM(revenue_amt) OVER (ORDER BY dt) AS revenue_cum,
        SUM(interest_amt + principal_amt) OVER (ORDER BY dt) AS repayment_cum,
        SUM(interest_rec_amt - writedown_amt) OVER (ORDER BY dt) AS net_gain_cum,
        SUM(deposit_amt + withdrawal_amt - allocation_amt) OVER (ORDER BY dt) AS net_deposit_cum,
        SUM(withdrawal_revenue_amt) OVER (ORDER BY dt) AS withdrawal_revenue_cum
    FROM events_daily
)

SELECT DISTINCT
    dt AS date,
    interest_amt AS interest_fees,
    interest_rec_amt AS supply_side_fees,
    revenue_amt AS interest_revenue,
    withdrawal_revenue_amt AS withdrawal_revenue,
    net_deposit_cum AS net_deposits,
    interest_rec_cum - writedown_cum + net_deposit_cum AS tvl,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM cumulative_calculations
