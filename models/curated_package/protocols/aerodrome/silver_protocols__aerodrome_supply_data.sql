{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'aerodrome', 'supply_data', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Aerodrome Supply Data

    Tracks daily Aerodrome token supply metrics including:
    - Pre-mine unlocks and emissions
    - Locked supply (veAERO balance)
    - Total and circulating supply
    - Buybacks (native and USD)
#}

WITH locked_supply_balance_partitioned AS (
    SELECT
        DATE(block_timestamp) AS date
        , balance_token / 1e18 AS ve_aero_balance
        , ROW_NUMBER() OVER (PARTITION BY DATE(block_timestamp) ORDER BY block_timestamp) AS rn
        , MAX(block_number) OVER (PARTITION BY DATE(block_timestamp)) AS block_number
        , MAX(modified_timestamp) OVER (PARTITION BY DATE(block_timestamp)) AS modified_timestamp
    FROM {{ ref('ez_base_address_balances_by_token') }}
    WHERE LOWER(contract_address) = LOWER('0x940181a94A35A4569E4529A3CDfB74e38FD98631')
        AND LOWER(address) = LOWER('0xeBf418Fe2512e7E6bd9b87a8F0f294aCDC67e6B4')
    {% if is_incremental() %}
        AND modified_timestamp >= (
            SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
            FROM {{ this }}
        )
        AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}
),

locked_supply_balance AS (
    SELECT
        date
        , ve_aero_balance
        , block_number
        , modified_timestamp
    FROM locked_supply_balance_partitioned
    WHERE rn = 1
),

emissions AS (
    SELECT
        DATE(block_timestamp) AS date
        , SUM(amount) AS emissions
        , MAX(block_number) AS block_number
        , MAX(modified_timestamp) AS modified_timestamp
    FROM {{ ref('core__ez_token_transfers') }}
    WHERE LOWER(contract_address) = LOWER('0x940181a94A35A4569E4529A3CDfB74e38FD98631')
        AND LOWER(from_address) = LOWER('0x0000000000000000000000000000000000000000')
    {% if is_incremental() %}
        AND modified_timestamp >= (
            SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
            FROM {{ this }}
        )
        AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}
    GROUP BY date
),

buybacks AS (
    SELECT
        DATE(block_timestamp) AS date
        , SUM(amount) AS buybacks_native
        , SUM(amount_usd) AS buybacks
        , MAX(block_number) AS block_number
        , MAX(modified_timestamp) AS modified_timestamp
    FROM {{ ref('core__ez_token_transfers') }}
    WHERE LOWER(from_address) IN (
            LOWER('0x834C0DA026d5F933C2c18Fa9F8Ba7f1f792fDa52'),
            LOWER('0xc27c8B3Ce02349f4916BFC8FD45A586D8787Ee5e'),
            LOWER('0xc9814f18a8751214F719De15C54D01b3D78EF14f')
        )
        AND LOWER(to_address) = LOWER('0xeBf418Fe2512e7E6bd9b87a8F0f294aCDC67e6B4')
    {% if is_incremental() %}
        AND modified_timestamp >= (
            SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
            FROM {{ this }}
        )
        AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}
    GROUP BY date
),

date_spine AS (
    SELECT date
    FROM {{ ref('dim_date_spine') }}
    WHERE date >= '2023-08-28' AND date <= CURRENT_DATE
    {% if is_incremental() %}
        AND date >= (SELECT MAX(date) - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days' FROM {{ this }})
    {% endif %}
)

SELECT
    ds.date
    , CASE WHEN ds.date = '2023-08-28' THEN COALESCE(em.emissions, 0) ELSE 0 END AS pre_mine_unlocks
    , CASE WHEN ds.date <> '2023-08-28' THEN COALESCE(em.emissions, 0) ELSE 0 END AS emissions_native
    , COALESCE(lsb.ve_aero_balance, 0) AS locked_supply
    , SUM(COALESCE(em.emissions, 0)) OVER (
        ORDER BY ds.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS total_supply
    , SUM(COALESCE(em.emissions, 0)) OVER (
        ORDER BY ds.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) - COALESCE(lsb.ve_aero_balance, 0) AS circulating_supply_native
    , COALESCE(bb.buybacks_native, 0) AS buybacks_native
    , COALESCE(bb.buybacks, 0) AS buybacks
    , GREATEST(COALESCE(lsb.block_number, 0), COALESCE(em.block_number, 0), COALESCE(bb.block_number, 0)) AS block_number
    , GREATEST(COALESCE(lsb.modified_timestamp, SYSDATE()), COALESCE(em.modified_timestamp, SYSDATE()), COALESCE(bb.modified_timestamp, SYSDATE())) AS modified_timestamp
    , SYSDATE() AS inserted_timestamp
    , '{{ invocation_id }}' AS _invocation_id
FROM date_spine AS ds
FULL JOIN emissions AS em
    ON ds.date = em.date
FULL JOIN locked_supply_balance AS lsb
    ON ds.date = lsb.date
FULL JOIN buybacks AS bb
    ON ds.date = bb.date
ORDER BY ds.date ASC
