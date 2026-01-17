{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['_log_id'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'aave', 'liquidation_revenue', 'curated']
) }}

{#
    Aave Liquidation Revenue - Consolidated Cross-Chain Model

    Captures liquidation events from Aave V2 and V3 deployments.
    Uses LiquidationCall events from pool contracts.
    Works across all chains - uses GLOBAL_PROJECT_NAME for chain identification.

    Deployments by chain:
    - V2: ethereum, polygon, avalanche
    - V3: ethereum, polygon, avalanche, arbitrum, optimism, base, gnosis, bsc
#}

{# Get pool address mappings from centralized vars #}
{% set v3_pools = vars.CURATED_AAVE_V3_POOLS %}
{% set v2_pools = vars.CURATED_AAVE_V2_POOLS %}

{# Get current chain and determine which versions are available #}
{% set chain = vars.GLOBAL_PROJECT_NAME %}
{% set has_v3 = chain in v3_pools %}
{% set has_v2 = chain in v2_pools %}

{# Build list of pool addresses to query #}
{% set pool_addresses = [] %}
{% if has_v3 %}
    {% do pool_addresses.append(v3_pools[chain]) %}
{% endif %}
{% if has_v2 %}
    {% do pool_addresses.append(v2_pools[chain]) %}
{% endif %}

WITH liquidator_events AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        decoded_log:liquidator::string AS liquidator,
        decoded_log:user::string AS user,
        COALESCE(decoded_log:collateralAsset::string, decoded_log:collateral::string) AS collateral_asset,
        decoded_log:liquidatedCollateralAmount::float AS liquidated_collateral_amount,
        COALESCE(decoded_log:debtAsset::string, decoded_log:principal::string) AS debt_asset,
        decoded_log:debtToCover::float AS debt_to_cover,
        modified_timestamp,
        CONCAT(tx_hash, '-', event_index) AS _log_id
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE event_name = 'LiquidationCall'
        AND contract_address IN (
            {% for addr in pool_addresses %}
            LOWER('{{ addr }}'){% if not loop.last %},{% endif %}
            {% endfor %}
        )
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
    {% endif %}
)

SELECT
    block_timestamp::date AS date,
    '{{ vars.GLOBAL_PROJECT_NAME }}' AS chain,
    CASE
        {% if has_v3 %}
        WHEN contract_address = LOWER('{{ v3_pools[chain] }}') THEN 'AAVE V3'
        {% endif %}
        {% if has_v2 %}
        WHEN contract_address = LOWER('{{ v2_pools[chain] }}') THEN 'AAVE V2'
        {% endif %}
    END AS protocol,
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    collateral_asset,
    liquidated_collateral_amount / POW(10, collateral_price.decimals) AS collateral_amount_nominal,
    collateral_amount_nominal * collateral_price.price AS collateral_amount_usd,
    debt_asset,
    debt_to_cover / POW(10, debt_price.decimals) AS debt_amount_nominal,
    debt_amount_nominal * debt_price.price AS debt_amount_usd,
    collateral_amount_usd - debt_amount_usd AS liquidation_revenue,
    l._log_id,
    l.modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM liquidator_events l
LEFT JOIN {{ ref('price__ez_prices_hourly') }} collateral_price
    ON LOWER(collateral_asset) = LOWER(collateral_price.token_address)
        AND date_trunc(hour, block_timestamp) = collateral_price.hour
LEFT JOIN {{ ref('price__ez_prices_hourly') }} debt_price
    ON LOWER(debt_asset) = LOWER(debt_price.token_address)
        AND date_trunc(hour, block_timestamp) = debt_price.hour
