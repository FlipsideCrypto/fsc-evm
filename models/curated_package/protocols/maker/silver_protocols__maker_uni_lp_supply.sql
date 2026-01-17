{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'maker', 'uni_lp_supply', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH token_transfers AS (
    SELECT
        DATE(block_timestamp) AS date,
        from_address,
        to_address,
        raw_amount_precise::NUMBER / 1e18 AS amount
    FROM {{ ref('core__ez_token_transfers') }}
    WHERE contract_address = LOWER('0x517F9dD285e75b599234F7221227339478d0FcC8')
    {% if is_incremental() %}
    AND block_timestamp >= DATEADD('day', -{{ vars.CURATED_LOOKBACK_DAYS }}, CURRENT_DATE())
    {% endif %}
),

daily_mints AS (
    SELECT
        date,
        SUM(amount) AS daily_minted
    FROM token_transfers
    WHERE from_address = LOWER('0x0000000000000000000000000000000000000000')
    GROUP BY date
),

daily_burns AS (
    SELECT
        date,
        SUM(amount) AS daily_burned
    FROM token_transfers
    WHERE to_address = LOWER('0x0000000000000000000000000000000000000000')
    GROUP BY date
),

dim_dates AS (
    SELECT
        date
    FROM {{ ref('dim_date_spine') }}
    WHERE date < TO_DATE(SYSDATE())
    {% if is_incremental() %}
    AND date >= DATEADD('day', -{{ vars.CURATED_LOOKBACK_DAYS }}, CURRENT_DATE())
    {% endif %}
),

daily_net_supply AS (
    SELECT
        d.date,
        COALESCE(m.daily_minted, 0) AS daily_minted,
        COALESCE(b.daily_burned, 0) AS daily_burned,
        COALESCE(m.daily_minted, 0) - COALESCE(b.daily_burned, 0) AS daily_net
    FROM dim_dates d
    LEFT JOIN daily_mints m ON d.date = m.date
    LEFT JOIN daily_burns b ON d.date = b.date
    WHERE d.date < TO_DATE(SYSDATE())
),

cumulative_supply AS (
    SELECT
        date,
        daily_minted,
        daily_burned,
        daily_net,
        SUM(daily_net) OVER (
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS circulating_supply
    FROM daily_net_supply
)

SELECT
    date,
    circulating_supply,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM cumulative_supply
