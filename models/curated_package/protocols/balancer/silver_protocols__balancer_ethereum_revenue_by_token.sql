{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'contract_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'balancer', 'revenue', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Balancer Ethereum Revenue by Token

    Tracks revenue collected by Balancer protocol fee collector.
    Fee collector address: 0xce88686553686DA562CE7Cea497CE749DA109f9F (Balancer v2 Protocol Fee Collector)
    Note: This is currently incomplete - there are missing fees.
#}

WITH base AS (
    SELECT
        block_timestamp::date AS date,
        contract_address,
        p.symbol AS token,
        SUM(raw_amount_precise::number / 1e18) AS amount_native,
        SUM(raw_amount_precise::number / 1e18 * p.price) AS amount_usd
    FROM {{ ref('core__ez_token_transfers') }} t
    LEFT JOIN {{ ref('price__ez_prices_hourly') }} p
        ON p.token_address = t.contract_address
        AND p.hour = t.block_timestamp::date
    WHERE to_address = LOWER('0xce88686553686DA562CE7Cea497CE749DA109f9F') -- Balancer v2 Protocol Fee Collector
    {% if is_incremental() %}
        AND block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
    {% endif %}
    GROUP BY 1, 2, 3
)

SELECT
    *,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM base
WHERE amount_usd < 1e6
