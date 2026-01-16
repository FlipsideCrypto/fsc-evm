{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'uniswap', 'token_incentives', 'curated'],
    enabled = true
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH prices AS (
    SELECT
        DATE(hour) AS date,
        AVG(price) AS price
    FROM {{ ref('price__ez_prices_hourly') }}
    WHERE LOWER(token_address) = LOWER('0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984')
    GROUP BY 1
),

incentives_v1 AS (
    SELECT
        DATE(l.block_timestamp) AS date,
        'UNI' AS token,
        SUM(l.decoded_log:reward::NUMBER / 1e18) AS reward_native,
        MAX(l.modified_timestamp) AS modified_timestamp
    FROM {{ ref('core__ez_decoded_event_logs') }} l
    WHERE
        contract_address IN (
            LOWER('0xca35e32e7926b96a9988f61d510e038108d8068e'),
            LOWER('0xa1484c3aa22a66c62b77e0ae78e15258bd0cb711'),
            LOWER('0x7fba4b8dc5e7616e59622806932dbea72537a56b'),
            LOWER('0x6c3e4cb2e96b01f4b866965a91ed4437839a121a')
        )
        AND event_name = 'RewardPaid'
        {% if is_incremental() %}
        AND l.modified_timestamp >= (
            SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
            FROM {{ this }}
        )
        AND l.modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
        {% endif %}
    GROUP BY 1, 2
),

incentives_v4 AS (
    SELECT
        date,
        'UNI' AS token,
        SUM(amount_native) AS reward_native
    FROM {{ ref('fact_uniswap_v4_token_incentives') }}
    {% if is_incremental() %}
    WHERE date >= (
        SELECT MAX(date) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
        FROM {{ this }}
    )
    {% endif %}
    GROUP BY 1, 2
),

total_incentives AS (
    SELECT
        date,
        token,
        reward_native AS reward_native_historic,
        0 AS reward_native_2025
    FROM incentives_v1

    UNION ALL

    SELECT
        date,
        token,
        0 AS reward_native_historic,
        reward_native AS reward_native_2025
    FROM incentives_v4
)

SELECT
    p.date,
    l.token,
    SUM(COALESCE(l.reward_native_historic, 0) + COALESCE(l.reward_native_2025, 0)) AS token_incentives_native,
    SUM(COALESCE((l.reward_native_historic + l.reward_native_2025) * p.price, 0)) AS token_incentives_usd,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM prices p
LEFT JOIN total_incentives l ON p.date = l.date
GROUP BY 1, 2
