{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    unique_key = 'ez_market_depth_stats_id',
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(ticker_id), SUBSTRING(ticker_id)",
    tags = ['gold','nado','curated']
) }}

SELECT
    hour,
    ticker_id,
    product_id,
    orderbook_side,
    volume,
    price,
    round_price_0_01,
    round_price_0_1,
    round_price_1,
    round_price_10,
    round_price_100,
    nado_market_depth_id as ez_market_depth_stats_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__nado_market_depth') }}
{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
        FROM
            {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
{% endif %}