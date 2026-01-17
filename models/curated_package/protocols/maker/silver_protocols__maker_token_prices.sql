{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['ts', 'token'],
    cluster_by = ['ts'],
    tags = ['silver_protocols', 'maker', 'token_prices', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH tokens AS (
    SELECT token, price_address
    FROM {{ ref('dim_treasury_erc20s') }}

    UNION ALL

    SELECT 'MKR' AS token, '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2' AS price_address

    UNION ALL

    SELECT 'ETH' AS token, '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS price_address
)

SELECT
    p.hour AS ts,
    t.token,
    p.price,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM {{ ref('price__ez_prices_hourly') }} p
INNER JOIN tokens t ON LOWER(p.token_address) = LOWER(t.price_address)
WHERE p.hour >= '2019-11-01'
{% if is_incremental() %}
AND p.hour >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
{% endif %}

UNION ALL

SELECT
    TIMESTAMP '2021-11-09 00:02' AS ts,
    'ENS' AS token,
    44.3 AS price,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
{% if is_incremental() %}
WHERE FALSE
{% endif %}
