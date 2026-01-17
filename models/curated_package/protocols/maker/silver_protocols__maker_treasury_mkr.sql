{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token', 'user_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'maker', 'treasury_mkr', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH prices AS (
    SELECT
        DATE(hour) AS date,
        token_address,
        symbol,
        AVG(price) AS price
    FROM {{ ref('price__ez_prices_hourly') }}
    WHERE token_address IN (
        LOWER('0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2'),
        LOWER('0x56072C95FAA701256059aa122697B133aDEd9279')
    )
    {% if is_incremental() %}
    AND hour >= DATEADD('day', -{{ vars.CURATED_LOOKBACK_DAYS }}, CURRENT_DATE())
    {% endif %}
    GROUP BY 1, 2, 3
),

mkr_balance_cte AS (
    SELECT
        DATE(block_timestamp) AS date,
        contract_address,
        user_address,
        MAX_BY(balance, block_timestamp) / 1e18 AS balance
    FROM {{ ref('core__fact_token_balances') }}
    WHERE user_address IN (
        LOWER('0xBE8E3e3618f7474F8cB1d074A26afFef007E98FB'),
        LOWER('0x8EE7D9235e01e6B42345120b5d270bdB763624C7'),
        LOWER('0x7Bb0b08587b8a6B8945e09F1Baca426558B0f06a')
    )
    AND contract_address IN (
        LOWER('0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2'),
        LOWER('0x56072C95FAA701256059aa122697B133aDEd9279')
    )
    {% if is_incremental() %}
    AND block_timestamp >= DATEADD('day', -{{ vars.CURATED_LOOKBACK_DAYS }}, CURRENT_DATE())
    {% endif %}
    GROUP BY 1, 2, 3
),

date_sequence AS (
    SELECT DISTINCT date
    FROM {{ ref('utils__date_spine') }}
    WHERE date BETWEEN (SELECT MIN(date) FROM mkr_balance_cte) AND TO_DATE(SYSDATE())
    {% if is_incremental() %}
    AND date >= DATEADD('day', -{{ vars.CURATED_LOOKBACK_DAYS }}, CURRENT_DATE())
    {% endif %}
),

user_addresses_contracts AS (
    SELECT DISTINCT user_address, contract_address
    FROM mkr_balance_cte
),

all_dates_users AS (
    SELECT
        d.date,
        contract_address,
        u.user_address
    FROM date_sequence d
    CROSS JOIN user_addresses_contracts u
),

joined_balances AS (
    SELECT
        a.date,
        a.contract_address,
        a.user_address,
        p.price,
        p.symbol,
        m.balance AS balance_token
    FROM all_dates_users a
    LEFT JOIN prices p ON p.date = a.date AND LOWER(p.token_address) = LOWER(a.contract_address)
    LEFT JOIN mkr_balance_cte m ON m.date = a.date AND m.user_address = a.user_address AND m.contract_address = a.contract_address
),

filled_balances AS (
    SELECT
        date,
        user_address,
        contract_address,
        price,
        symbol,
        COALESCE(
            balance_token,
            LAST_VALUE(balance_token IGNORE NULLS) OVER (
                PARTITION BY user_address, contract_address
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS balance_token,
        COALESCE(
            user_address,
            LAST_VALUE(user_address IGNORE NULLS) OVER (
                PARTITION BY user_address, contract_address
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS filled_user_address
    FROM joined_balances
)

SELECT
    date,
    price,
    user_address,
    IFF(symbol = 'SKY', symbol, 'MKR') AS token,
    SUM(balance_token) AS amount_native,
    SUM(balance_token * price) AS amount_usd,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM filled_balances
GROUP BY date, token, price, user_address
