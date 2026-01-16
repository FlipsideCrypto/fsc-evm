{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'strategy_address', 'token_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'eigenlayer', 'restaked_tokens', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Eigenlayer Restaked Tokens

    Tracks daily token balances for Eigenlayer strategies. Creates a time series
    of token balances across all strategy-token combinations, forward-filling
    missing data points with the last known balance.
#}

WITH DepositsIntoStrategy AS (
    SELECT
        DATE(block_timestamp) AS day,
        from_address,
        decoded_input_data:strategy::STRING AS strategy_address,
        decoded_input_data:token::STRING AS token_address,
        SUM(CAST(decoded_input_data:amount AS BIGINT)) AS total_deposited
    FROM {{ ref('core__ez_decoded_traces') }}
    WHERE TO_ADDRESS = LOWER('0x858646372CC42E1A627fcE94aa7A7033e7CF075A')
    AND FUNCTION_NAME = 'depositIntoStrategy'
    {% if is_incremental() %}
    AND block_timestamp >= (SELECT MAX(date) FROM {{ this }})
    {% endif %}
    GROUP BY 1, 2, 3, 4
),

BalanceEntries AS (
    SELECT
        block_timestamp,
        DATE(block_timestamp) AS day,
        address AS strategy_address,
        contract_address AS token_address,
        balance_token,
        ROW_NUMBER() OVER (
            PARTITION BY DATE(block_timestamp), address, contract_address
            ORDER BY block_timestamp DESC
        ) AS latest_balance_rank
    FROM {{ ref('fact_ethereum_address_balances_by_token') }}
    {% if is_incremental() %}
    WHERE block_timestamp >= (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),

LatestDailyBalances AS (
    SELECT
        day,
        strategy_address,
        token_address,
        balance_token
    FROM BalanceEntries
    WHERE latest_balance_rank = 1
),

Dates AS (
    SELECT
        date
    FROM {{ ref('dim_date_spine') }}
    WHERE date BETWEEN '2023-12-01' AND TO_DATE(SYSDATE())
    {% if is_incremental() %}
    AND date >= (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),

StrategyTokenCombinations AS (
    SELECT DISTINCT
        strategy_address,
        token_address
    FROM DepositsIntoStrategy
),

DateStrategyTokenCombinations AS (
    SELECT
        d.date,
        stc.strategy_address,
        stc.token_address
    FROM Dates d
    CROSS JOIN StrategyTokenCombinations stc
),

FinalResult AS (
    SELECT
        dst.date,
        dst.strategy_address,
        dst.token_address,
        ldb.balance_token
    FROM DateStrategyTokenCombinations dst
    LEFT JOIN LatestDailyBalances ldb
        ON dst.date = ldb.day
        AND dst.strategy_address = ldb.strategy_address
        AND dst.token_address = ldb.token_address
),

FrontFilledBalances AS (
    SELECT
        date,
        strategy_address,
        token_address,
        COALESCE(
            balance_token,
            LAST_VALUE(balance_token) IGNORE NULLS OVER (
                PARTITION BY strategy_address, token_address
                ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ),
            0 -- If no historic record exists, set balance_token to 0
        ) AS balance_token_filled
    FROM FinalResult
)

SELECT
    date,
    strategy_address,
    token_address,
    balance_token_filled AS balance_token,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM FrontFilledBalances
ORDER BY date ASC
