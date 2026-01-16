{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'contract_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'convex', 'incentives', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Convex Token Incentives

    Tracks CVX token mints (incentives) distributed since May 17, 2021.
    CVX Token Address: 0x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b
#}

SELECT
    block_timestamp::DATE AS date,
    t.contract_address,
    t.symbol,
    SUM(raw_amount_precise / POW(10, 18)) AS token_incentives_native,
    SUM(raw_amount_precise / POW(10, 18) * p.price) AS token_incentives,
    MAX(t.block_number) AS block_number,
    MAX(t.modified_timestamp) AS modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM {{ ref('core__ez_token_transfers') }} t
LEFT JOIN {{ ref('price__ez_prices_hourly') }} p
    ON p.hour = block_timestamp::DATE
    AND p.token_address = t.contract_address
WHERE contract_address = LOWER('0x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b')
    AND from_address = LOWER('0x0000000000000000000000000000000000000000')
    AND DATE(block_timestamp) > '2021-05-17'
{% if is_incremental() %}
    AND t.modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
        FROM {{ this }}
    )
    AND t.modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
{% endif %}
GROUP BY 1, 2, 3
ORDER BY 1 DESC
