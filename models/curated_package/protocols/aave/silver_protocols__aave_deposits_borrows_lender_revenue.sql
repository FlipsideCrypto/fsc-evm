{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token_address', 'protocol'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'aave', 'deposits_borrows_lender_revenue', 'curated']
) }}

{#
    Aave Deposits, Borrows, and Lender Revenue - Consolidated Cross-Chain Model

    Tracks lending metrics from Aave pools:
    - Daily borrow rates (from ReserveDataUpdated events)
    - Daily liquidity rates
    - Reserve factors (from ReserveFactorChanged events)

    Works across all chains - uses GLOBAL_PROJECT_NAME for chain identification.

    Deployments by chain:
    - V2: ethereum, polygon, avalanche
    - V3: ethereum, polygon, avalanche, arbitrum, optimism, base, gnosis, bsc
#}

{# Get contract address mappings from centralized vars #}
{% set v3_pools = vars.CURATED_AAVE_V3_POOLS %}
{% set v2_pools = vars.CURATED_AAVE_V2_POOLS %}
{% set v3_collectors = vars.CURATED_AAVE_V3_COLLECTORS %}
{% set v2_collectors = vars.CURATED_AAVE_V2_COLLECTORS %}

{# Get current chain and determine which versions are available #}
{% set chain = vars.GLOBAL_PROJECT_NAME %}
{% set has_v3 = chain in v3_pools %}
{% set has_v2 = chain in v2_pools %}

{# Build list of pool and collector addresses to query #}
{% set pool_addresses = [] %}
{% set collector_addresses = [] %}
{% if has_v3 %}
    {% do pool_addresses.append(v3_pools[chain]) %}
    {% do collector_addresses.append(v3_collectors[chain]) %}
{% endif %}
{% if has_v2 %}
    {% do pool_addresses.append(v2_pools[chain]) %}
    {% if v2_collectors[chain] not in collector_addresses %}
        {% do collector_addresses.append(v2_collectors[chain]) %}
    {% endif %}
{% endif %}

WITH average_liquidity_rate AS (
    SELECT
        block_number,
        block_timestamp::date AS date,
        contract_address AS pool_address,
        decoded_log:reserve::string AS reserve,
        AVG(decoded_log:stableBorrowRate::float / 1e27) AS stable_borrow_rate,
        AVG(decoded_log:variableBorrowIndex::float / 1e27) AS borrow_index,
        AVG(decoded_log:liquidityIndex::float / 1e27) AS liquidity_index,
        MAX(modified_timestamp) AS modified_timestamp
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE contract_address IN (
            {% for addr in pool_addresses %}
            LOWER('{{ addr }}'){% if not loop.last %},{% endif %}
            {% endfor %}
        )
        AND event_name = 'ReserveDataUpdated'
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
    {% endif %}
    GROUP BY 1, 2, 3, 4
),

reserve_factor_data AS (
    SELECT
        block_timestamp::date AS date,
        decoded_log:asset::string AS reserve,
        MAX(COALESCE(decoded_log:newReserveFactor::number, decoded_log:factor::number)) / 1E4 AS reserve_factor,
        MAX(decoded_log:oldReserveFactor::number) / 1E4 AS old_reserve_factor
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE contract_address IN (
            {% for addr in collector_addresses %}
            LOWER('{{ addr }}'){% if not loop.last %},{% endif %}
            {% endfor %}
        )
        AND event_name = 'ReserveFactorChanged'
    GROUP BY 1, 2
),

dates AS (
    SELECT DISTINCT block_timestamp::date AS date
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE date >= (SELECT MIN(date) FROM reserve_factor_data)
),

cross_join_reserve_dates AS (
    SELECT
        reserve,
        date
    FROM dates
    CROSS JOIN (
        SELECT DISTINCT reserve
        FROM reserve_factor_data
    )
),

forward_filled_reserve_factor AS (
    SELECT
        date,
        reserve,
        COALESCE(
            reserve_factor,
            LAG(reserve_factor) IGNORE NULLS OVER (PARTITION BY reserve ORDER BY date)
        ) AS reserve_factor,
        COALESCE(
            old_reserve_factor,
            LAG(old_reserve_factor) IGNORE NULLS OVER (PARTITION BY reserve ORDER BY date)
        ) AS old_reserve_factor
    FROM cross_join_reserve_dates
    LEFT JOIN reserve_factor_data USING(date, reserve)
),

daily_rate AS (
    SELECT
        a.block_number,
        a.date,
        a.pool_address,
        a.reserve,
        stable_borrow_rate/365 AS stable_borrow_rate,
        (borrow_index /
            CASE
                WHEN DATEADD(day, -1, a.date) = LAG(a.date) OVER (PARTITION BY a.reserve, a.pool_address ORDER BY a.date)
                THEN LAG(borrow_index) OVER (PARTITION BY a.reserve, a.pool_address ORDER BY a.date)
                ELSE borrow_index
            END
        ) - 1 AS daily_borrow_rate,
        (liquidity_index /
            CASE
                WHEN DATEADD(day, -1, a.date) = LAG(a.date) OVER (PARTITION BY a.reserve, a.pool_address ORDER BY a.date)
                THEN LAG(liquidity_index) OVER (PARTITION BY a.reserve, a.pool_address ORDER BY a.date)
                ELSE liquidity_index
            END
        ) - 1 AS daily_liquidity_rate,
        COALESCE(
            f.reserve_factor,
            f.old_reserve_factor,
            0
        ) AS reserve_factor,
        a.modified_timestamp
    FROM average_liquidity_rate a
    LEFT JOIN forward_filled_reserve_factor f USING(date, reserve)
)

SELECT
    date,
    reserve AS token_address,
    '{{ vars.GLOBAL_PROJECT_NAME }}' AS chain,
    CASE
        {% if has_v3 %}
        WHEN pool_address = LOWER('{{ v3_pools[chain] }}') THEN 'AAVE V3'
        {% endif %}
        {% if has_v2 %}
        WHEN pool_address = LOWER('{{ v2_pools[chain] }}') THEN 'AAVE V2'
        {% endif %}
    END AS protocol,
    daily_borrow_rate,
    daily_liquidity_rate,
    stable_borrow_rate,
    reserve_factor,
    block_number,
    modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM daily_rate
