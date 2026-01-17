{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'maker', 'treasury_lp', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH dates AS (
    SELECT
        DISTINCT DATE(hour) AS date
    FROM {{ ref('price__ez_prices_hourly') }}
    WHERE symbol = 'MKR'
    {% if is_incremental() %}
    AND hour >= DATEADD('day', -{{ vars.CURATED_LOOKBACK_DAYS }}, CURRENT_DATE())
    {% endif %}
),

treasury_balance AS (
    SELECT
        DATE(block_timestamp) AS date,
        MAX(balance)::NUMBER / 1e18 AS treasury_lp_balance
    FROM
        {{ ref('core__fact_token_balances') }}
    WHERE
        contract_address = LOWER('0x517F9dD285e75b599234F7221227339478d0FcC8')
        AND user_address = LOWER('0xBE8E3e3618f7474F8cB1d074A26afFef007E98FB')
    {% if is_incremental() %}
        AND block_timestamp >= DATEADD('day', -{{ vars.CURATED_LOOKBACK_DAYS }}, CURRENT_DATE())
    {% endif %}
    GROUP BY 1
),

value_per_token_cte AS (
    SELECT
        s.date,
        v.amount_usd / s.circulating_supply AS value_per_token_usd,
        s.circulating_supply
    FROM
        {{ ref('silver_protocols__maker_uni_lp_supply') }} s
        LEFT JOIN {{ ref('silver_protocols__maker_uni_lp_value') }} v ON v.date = s.date
    WHERE
        value_per_token_usd IS NOT NULL
    {% if is_incremental() %}
        AND s.date >= DATEADD('day', -{{ vars.CURATED_LOOKBACK_DAYS }}, CURRENT_DATE())
    {% endif %}
),

filled_data AS (
    SELECT
        d.date,
        LAST_VALUE(t.treasury_lp_balance IGNORE NULLS) OVER (
            ORDER BY d.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS amount_native,
        LAST_VALUE(v.value_per_token_usd IGNORE NULLS) OVER (
            ORDER BY d.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS value_per_token_usd
    FROM
        dates d
        LEFT JOIN treasury_balance t ON d.date = t.date
        LEFT JOIN value_per_token_cte v ON d.date = v.date
)

SELECT
    date,
    amount_native,
    value_per_token_usd,
    amount_native * value_per_token_usd AS amount_usd,
    'UNI V2: DAI-MKR' AS token,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    filled_data
WHERE amount_native IS NOT NULL
