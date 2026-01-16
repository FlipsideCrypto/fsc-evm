{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'uniswap', 'treasury', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH dates AS (
    SELECT
        DISTINCT DATE(hour) AS date
    FROM {{ ref('price__ez_prices_hourly') }}
    WHERE symbol = 'UNI'
),

treasury_addresses AS (
    SELECT addresses
    FROM (
        VALUES
            (LOWER('0x1a9C8182C09F50C8318d769245beA52c32BE35BC')),
            (LOWER('0x3D30B1aB88D487B0F3061F40De76845Bec3F1e94')),
            (LOWER('0x4750c43867EF5F89869132ecCF19B9b6C4286E1a')),
            (LOWER('0x4b4e140D1f131fdaD6fb59C13AF796fD194e4135')),
            (LOWER('0xe3953D9d317B834592aB58AB2c7A6aD22b54075D'))
    ) AS treasury_addresses(addresses)
),

tokens AS (
    -- No incremental filter here: we need ALL tokens ever transferred to treasury
    -- The incremental filter is applied only to sparse_balances for balance lookups
    SELECT DISTINCT LOWER(contract_address) AS token_address
    FROM {{ ref('core__ez_token_transfers') }}
    WHERE LOWER(to_address) IN (
        SELECT addresses
        FROM treasury_addresses
    )
),

sparse_balances AS (
    SELECT
        DATE(block_timestamp) AS date,
        user_address,
        contract_address,
        MAX_BY(balance, block_timestamp) / 1e18 AS balance_daily
    FROM {{ ref('core__fact_token_balances') }}
    WHERE
        LOWER(contract_address) IN (
            SELECT token_address
            FROM tokens
        )
        AND LOWER(user_address) IN (
            SELECT addresses
            FROM treasury_addresses
        )
        {% if is_incremental() %}
        AND modified_timestamp >= (
            SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
            FROM {{ this }}
        )
        AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
        {% endif %}
    GROUP BY 1, 2, 3
),

full_balances AS (
    SELECT
        d.date,
        ta.addresses AS user_address,
        t.token_address AS contract_address,
        COALESCE(
            LAST_VALUE(sb.balance_daily) IGNORE NULLS OVER (
                PARTITION BY ta.addresses, t.token_address
                ORDER BY d.date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ),
            0
        ) AS balance_daily
    FROM dates d
    CROSS JOIN treasury_addresses ta
    CROSS JOIN tokens t
    LEFT JOIN sparse_balances sb
        ON d.date = sb.date
        AND ta.addresses = sb.user_address
        AND t.token_address = sb.contract_address
),

daily_prices AS (
    SELECT
        DATE(hour) AS date,
        token_address,
        symbol,
        AVG(price) AS avg_daily_price,
        MAX(decimals) AS decimals
    FROM {{ ref('price__ez_prices_hourly') }}
    WHERE token_address IN (
        SELECT token_address
        FROM tokens
    )
    GROUP BY 1, 2, 3
),

full_table AS (
    SELECT
        fb.date,
        fb.user_address,
        fb.contract_address,
        dp.symbol,
        fb.balance_daily AS balance_daily,
        COALESCE(dp.avg_daily_price, 0) AS avg_daily_price,
        fb.balance_daily * COALESCE(dp.avg_daily_price, 0) AS usd_balance
    FROM full_balances fb
    LEFT JOIN daily_prices dp
        ON fb.date = dp.date
        AND fb.contract_address = dp.token_address
    WHERE symbol IS NOT NULL
)

SELECT
    date,
    symbol AS token,
    SUM(balance_daily) AS treasury_native,
    SUM(usd_balance) AS usd_balance,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM full_table
WHERE usd_balance > 1
GROUP BY 1, 2
