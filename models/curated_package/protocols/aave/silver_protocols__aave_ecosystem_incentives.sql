{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token_address', 'protocol'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'aave', 'ecosystem_incentives', 'curated']
) }}

{#
    Aave Ecosystem Incentives - Consolidated Cross-Chain Model

    Tracks reward distributions from Aave incentives controllers.
    Captures RewardsClaimed events for liquidity mining rewards.
    Works across all chains - uses GLOBAL_PROJECT_NAME for chain identification.

    Deployments by chain:
    - V2: ethereum, polygon, avalanche
    - V3: ethereum, polygon, avalanche, arbitrum, optimism, base, gnosis, bsc
#}

{# Get incentives controller address mappings from centralized vars #}
{% set v3_incentives = vars.CURATED_AAVE_V3_INCENTIVES %}
{% set v2_incentives = vars.CURATED_AAVE_V2_INCENTIVES %}

{# Get current chain and determine which versions are available #}
{% set chain = vars.GLOBAL_PROJECT_NAME %}
{% set has_v3 = chain in v3_incentives %}
{% set has_v2 = chain in v2_incentives %}

{# Build list of incentives controller addresses to query #}
{% set incentives_addresses = [] %}
{% if has_v3 %}
    {% do incentives_addresses.append(v3_incentives[chain]) %}
{% endif %}
{% if has_v2 and (not has_v3 or v2_incentives[chain] != v3_incentives[chain]) %}
    {# Only add V2 if it's different from V3 to avoid duplicates #}
    {% do incentives_addresses.append(v2_incentives[chain]) %}
{% endif %}

WITH claimed_rewards AS (
    SELECT
        block_number,
        block_timestamp::date AS date,
        contract_address,
        decoded_log:reward::string AS reward_token,
        decoded_log:amount::float AS amount,
        modified_timestamp
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE contract_address IN (
            {% for addr in incentives_addresses %}
            LOWER('{{ addr }}'){% if not loop.last %},{% endif %}
            {% endfor %}
        )
        AND event_name = 'RewardsClaimed'
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
        c.date,
        c.block_number,
        c.contract_address,
        c.reward_token AS token_address,
        amount / POW(10, p.decimals) AS amount_nominal,
        amount_nominal * p.price AS amount_usd,
        c.modified_timestamp
    FROM claimed_rewards c
    LEFT JOIN {{ ref('price__ez_prices_hourly') }} p
        ON LOWER(c.reward_token) = LOWER(p.token_address)
        AND date_trunc(hour, c.date) = p.hour
)

SELECT
    date,
    '{{ vars.GLOBAL_PROJECT_NAME }}' AS chain,
    CASE
        {% if has_v3 %}
        WHEN contract_address = LOWER('{{ v3_incentives[chain] }}') THEN 'AAVE V3'
        {% endif %}
        {% if has_v2 and (not has_v3 or v2_incentives[chain] != v3_incentives[chain]) %}
        WHEN contract_address = LOWER('{{ v2_incentives[chain] }}') THEN 'AAVE V2'
        {% endif %}
        {% if has_v2 and has_v3 and v2_incentives[chain] == v3_incentives[chain] %}
        {# If V2 and V3 share the same address, label as combined #}
        WHEN contract_address = LOWER('{{ v3_incentives[chain] }}') THEN 'AAVE V2/V3'
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
