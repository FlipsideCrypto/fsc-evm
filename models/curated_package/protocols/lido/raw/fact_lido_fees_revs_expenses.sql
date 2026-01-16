{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'lido', 'fees_revs_expenses', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Protocol-specific addresses #}
{% set steth_address = vars.PROTOCOL_LIDO_STETH_ADDRESS %}
{% set treasury_address = vars.PROTOCOL_LIDO_TREASURY %}

{#
    Lido Fees, Revenues, and Expenses

    Calculates daily:
    - Total staking yield (block rewards + MEV)
    - Fee allocations (treasury, node operators)
    - Supply side revenue (staker earnings)
#}

WITH
steth_prices AS (
    SELECT
        hour
        , price
    FROM {{ ref('price__ez_prices_hourly') }}
    WHERE token_address = LOWER('{{ steth_address }}')
),
eth_prices AS (
    SELECT
        hour
        , price
    FROM {{ ref('price__ez_prices_hourly') }}
    WHERE symbol = 'ETH' AND is_native = TRUE
),
fees_revs_expenses AS (
    SELECT
        DATE(block_timestamp) AS date
        , 'ethereum' AS chain
        , 'stETH' AS token
        , SUM(raw_amount_precise::number / 1e18 / (f.treasury_fee_pct + f.insurance_fee_pct + f.operators_fee_pct + 0.90)) AS total_staking_yield_native
        , SUM(raw_amount_precise::number / 1e18 / (f.treasury_fee_pct + f.insurance_fee_pct + f.operators_fee_pct + 0.90) * p.price) AS total_staking_yield_usd
        , AVG(f.treasury_fee_pct) AS treasury_fee_pct
        , AVG(f.insurance_fee_pct) AS insurance_fee_pct
        , AVG(f.operators_fee_pct) AS operators_fee_pct
        , MAX(t.block_number) AS block_number
        , MAX(t.modified_timestamp) AS modified_timestamp
    FROM {{ ref('core__ez_token_transfers') }} t
    LEFT JOIN steth_prices p ON DATE_TRUNC('hour', t.block_timestamp) = p.hour
    LEFT JOIN {{ ref('fact_lido_fee_split') }} f ON t.block_timestamp::date = f.date
    WHERE contract_address = LOWER('{{ steth_address }}')
        AND from_address = LOWER('0x0000000000000000000000000000000000000000')
        AND to_address = LOWER('{{ treasury_address }}')
        AND origin_function_signature <> LOWER('0xf98a4eca')
    {% if is_incremental() %}
        AND t.modified_timestamp >= (
            SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
            FROM {{ this }}
        )
        AND t.modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}
    GROUP BY 1
),
mev AS (
    SELECT
        DATE(l.block_timestamp) AS date
        , COALESCE(utils.udf_hex_to_int(data)::number, 0) / 1e18 AS mev_priority_fees_amount_eth
        , COALESCE(utils.udf_hex_to_int(data)::number, 0) / 1e18 * COALESCE(p.price, 0) AS mev_priority_fees_amount_usd
        , MAX(l.block_number) AS block_number
        , MAX(l.modified_timestamp) AS modified_timestamp
    FROM {{ ref('core__fact_event_logs') }} l
    LEFT JOIN eth_prices p ON p.hour = DATE_TRUNC('hour', l.block_timestamp)
    WHERE topics[0] = '0xd27f9b0c98bdee27044afa149eadcd2047d6399cb6613a45c5b87e6aca76e6b5'
    {% if is_incremental() %}
        AND l.modified_timestamp >= (
            SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
            FROM {{ this }}
        )
        AND l.modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}
    GROUP BY 1, 2, 3
)

SELECT
    f.date
    , 'ethereum' AS chain
    , 'Lido' AS protocol
    -- NATIVE
    , 'ETH' AS symbol
    , COALESCE(f.total_staking_yield_native, 0) - COALESCE(m.mev_priority_fees_amount_eth, 0) AS block_rewards_native
    , COALESCE(m.mev_priority_fees_amount_eth, 0) AS mev_priority_fees_native
    , COALESCE(f.total_staking_yield_native, 0) AS total_staking_yield_native
    , COALESCE(f.total_staking_yield_native, 0) * (f.operators_fee_pct + f.treasury_fee_pct + f.insurance_fee_pct) AS fees_native
    , COALESCE(f.total_staking_yield_native, 0) * f.operators_fee_pct AS validator_fee_allocation_native
    , COALESCE(f.total_staking_yield_native, 0) * (f.treasury_fee_pct + f.insurance_fee_pct) AS treasury_fee_allocation_native
    , COALESCE(f.total_staking_yield_native, 0) * (f.treasury_fee_pct + f.insurance_fee_pct) AS protocol_revenue_native
    , (COALESCE(f.total_staking_yield_native, 0) - COALESCE(m.mev_priority_fees_amount_eth, 0)) * 0.90 AS primary_supply_side_revenue_native
    , COALESCE(m.mev_priority_fees_amount_eth, 0) * 0.90 AS secondary_supply_side_revenue_native
    , COALESCE(f.total_staking_yield_native, 0) * 0.90 AS total_supply_side_revenue_native
    -- USD
    , COALESCE(f.total_staking_yield_usd, 0) - COALESCE(m.mev_priority_fees_amount_usd, 0) AS block_rewards
    , COALESCE(m.mev_priority_fees_amount_usd, 0) AS mev_priority_fees
    , COALESCE(f.total_staking_yield_usd, 0) AS total_staking_yield
    , COALESCE(f.total_staking_yield_usd, 0) * (f.operators_fee_pct + f.treasury_fee_pct + f.insurance_fee_pct) AS fees
    , COALESCE(f.total_staking_yield_usd, 0) * f.operators_fee_pct AS validator_fee_allocation
    , COALESCE(f.total_staking_yield_usd, 0) * (f.treasury_fee_pct + f.insurance_fee_pct) AS treasury_fee_allocation
    , COALESCE(f.total_staking_yield_usd, 0) * (f.treasury_fee_pct + f.insurance_fee_pct) AS protocol_revenue
    , (COALESCE(f.total_staking_yield_usd, 0) - COALESCE(m.mev_priority_fees_amount_usd, 0)) * 0.90 AS primary_supply_side_revenue
    , COALESCE(m.mev_priority_fees_amount_usd, 0) * 0.90 AS secondary_supply_side_revenue
    , COALESCE(f.total_staking_yield_usd, 0) * 0.90 AS total_supply_side_revenue
    , GREATEST(COALESCE(f.block_number, 0), COALESCE(m.block_number, 0)) AS block_number
    , GREATEST(COALESCE(f.modified_timestamp, SYSDATE()), COALESCE(m.modified_timestamp, SYSDATE())) AS modified_timestamp
FROM fees_revs_expenses f
LEFT JOIN mev m ON m.date = f.date
