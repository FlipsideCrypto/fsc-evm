{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token_address', 'protocol'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'aave', 'reserve_factor_revenue', 'curated']
) }}

{#
    Aave Reserve Factor Revenue - Consolidated Cross-Chain Model

    Tracks reserve factor revenue from ReserveDataUpdated events.
    The amountToTreasury field indicates revenue accrued to the protocol treasury.
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

WITH reserve_factor_events AS (
    SELECT
        block_number,
        block_timestamp::date AS date,
        contract_address,
        decoded_log:asset::string AS reserve,
        decoded_log:amountToTreasury::float AS amount_to_treasury,
        modified_timestamp
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE contract_address IN (
            {% for addr in pool_addresses %}
            LOWER('{{ addr }}'){% if not loop.last %},{% endif %}
            {% endfor %}
        )
        AND event_name = 'ReserveDataUpdated'
        AND decoded_log:amountToTreasury IS NOT NULL
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
    {% endif %}
),

priced AS (
    SELECT
        r.date,
        r.block_number,
        r.contract_address,
        r.reserve AS token_address,
        amount_to_treasury / POW(10, p.decimals) AS amount_nominal,
        amount_nominal * p.price AS amount_usd,
        r.modified_timestamp
    FROM reserve_factor_events r
    LEFT JOIN {{ ref('price__ez_prices_hourly') }} p
        ON LOWER(r.reserve) = LOWER(p.token_address)
        AND date_trunc(hour, r.date) = p.hour
)

SELECT
    date,
    '{{ vars.GLOBAL_PROJECT_NAME }}' AS chain,
    CASE
        {% if has_v3 %}
        WHEN contract_address = LOWER('{{ v3_pools[chain] }}') THEN 'AAVE V3'
        {% endif %}
        {% if has_v2 %}
        WHEN contract_address = LOWER('{{ v2_pools[chain] }}') THEN 'AAVE V2'
        {% endif %}
    END AS protocol,
    token_address,
    SUM(COALESCE(amount_nominal, 0)) AS amount_nominal,
    SUM(COALESCE(amount_usd, 0)) AS amount_usd,
    MAX(block_number) AS block_number,
    MAX(modified_timestamp) AS modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM priced
GROUP BY 1, 2, 3, 4
