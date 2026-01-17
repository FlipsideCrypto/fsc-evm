{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['ts', 'token'],
    cluster_by = ['ts'],
    tags = ['silver_protocols', 'maker', 'm2m_levels', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH treasury_tokens AS (
    SELECT token, price_address
    FROM {{ ref('dim_treasury_erc20s') }}

    UNION ALL

    SELECT 'DAI' AS token, '0x6b175474e89094c44da98b954eedeac495271d0f' AS price_address
)

SELECT
    p.hour AS ts,
    tt.token,
    CASE WHEN tt.token = 'DAI' THEN 1 ELSE p.price END AS price,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM {{ ref('price__ez_prices_hourly') }} p
INNER JOIN treasury_tokens tt ON p.token_address = tt.price_address
WHERE p.hour >= '2019-11-01'
  AND EXTRACT(HOUR FROM p.hour) = 23
{% if is_incremental() %}
  AND p.hour >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
{% endif %}
