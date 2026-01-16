{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'address', 'token_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'convex', 'tvl', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Convex Staked TVL by Token

    Tracks daily staked token balances in Convex via the Voter Proxy.
    Voter Proxy Address: 0x989aeb4d175e16225e39e87d0d97a3360524ad80
    Maps gauge tokens to LP tokens via Convex pools.
#}

WITH lp_to_gauge AS (
    SELECT
        lptoken AS lp_token,
        gauge
    FROM {{ ref('silver_protocols__convex_pools') }}
),

eod_address_token_balances AS (
    SELECT
        block_timestamp::DATE AS date,
        address,
        lp.lp_token AS contract_address,
        MAX_BY(balance_token, block_timestamp) AS eod_balance,
        MAX(modified_timestamp) AS modified_timestamp
    FROM {{ ref('fact_ethereum_address_balances_by_token') }}
    LEFT JOIN lp_to_gauge lp ON lp.gauge = contract_address
    WHERE LOWER(address) = LOWER('0x989aeb4d175e16225e39e87d0d97a3360524ad80')
        AND contract_address IN (
            SELECT gauge
            FROM lp_to_gauge
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

distinct_address_tokens AS (
    SELECT DISTINCT
        address,
        contract_address
    FROM eod_address_token_balances
),

date_address_token_spine AS (
    SELECT DISTINCT
        ds.date,
        cp.address,
        cp.contract_address
    FROM {{ ref('dim_date_spine') }} ds
    CROSS JOIN distinct_address_tokens cp
    WHERE ds.date BETWEEN '2020-02-29' AND TO_DATE(SYSDATE())
),

sparse_balances AS (
    SELECT
        dats.date,
        dats.address,
        dats.contract_address,
        b.eod_balance,
        b.modified_timestamp
    FROM date_address_token_spine dats
    LEFT JOIN eod_address_token_balances b
        USING (date, address, contract_address)
),

filled_balances AS (
    SELECT
        date,
        address,
        contract_address,
        COALESCE(
            eod_balance,
            LAST_VALUE(eod_balance IGNORE NULLS) OVER (
                PARTITION BY address, contract_address
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS daily_balance,
        COALESCE(
            modified_timestamp,
            LAST_VALUE(modified_timestamp IGNORE NULLS) OVER (
                PARTITION BY address, contract_address
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS modified_timestamp
    FROM sparse_balances
)

SELECT
    b.date,
    b.address,
    b.contract_address AS token_address,
    cp.coin_0,
    cp.coin_1,
    c.name,
    b.daily_balance / POW(10, 18) AS balance_native,
    b.modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM filled_balances b
LEFT JOIN {{ ref('core__dim_contracts') }} c ON c.address = b.contract_address
LEFT JOIN {{ ref('dim_curve_pools') }} cp ON LOWER(cp.token) = LOWER(b.contract_address)
WHERE b.daily_balance IS NOT NULL
