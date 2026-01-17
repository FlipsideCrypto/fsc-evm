{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'maker', 'uni_lp_value', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH token_balances AS (
    SELECT
        DATE(block_timestamp) AS date,
        CASE
            WHEN contract_address = LOWER('0x6B175474E89094C44Da98b954EedeAC495271d0F') THEN 'DAI'
            WHEN contract_address = LOWER('0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2') THEN 'MKR'
        END AS token,
        MAX_BY(balance, block_timestamp)::NUMBER / 1e18 AS balance
    FROM
        {{ ref('core__fact_token_balances') }}
    WHERE
        user_address = LOWER('0x517F9dD285e75b599234F7221227339478d0FcC8')
        AND contract_address IN (
            LOWER('0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2'),
            LOWER('0x6B175474E89094C44Da98b954EedeAC495271d0F')
        )
    {% if is_incremental() %}
        AND block_timestamp >= DATEADD('day', -{{ vars.CURATED_LOOKBACK_DAYS }}, CURRENT_DATE())
    {% endif %}
    GROUP BY
        1, 2
),

prices AS (
    SELECT
        DATE(hour) AS date,
        MAX_BY(price, hour) AS price,
        symbol
    FROM
        {{ ref('price__ez_prices_hourly') }}
    WHERE
        token_address IN (
            LOWER('0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2'),
            LOWER('0x6B175474E89094C44Da98b954EedeAC495271d0F')
        )
    {% if is_incremental() %}
        AND hour >= DATEADD('day', -{{ vars.CURATED_LOOKBACK_DAYS }}, CURRENT_DATE())
    {% endif %}
    GROUP BY
        1, 3
),

daily_prices_by_token AS (
    SELECT
        b.date,
        p.price * b.balance AS balance_usd,
        b.token
    FROM
        token_balances b
        LEFT JOIN prices p ON p.date = b.date
        AND p.symbol = b.token
)

SELECT
    date,
    SUM(balance_usd) AS amount_usd,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    daily_prices_by_token
GROUP BY 1
