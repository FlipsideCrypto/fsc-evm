{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    unique_key = 'ez_market_stats_id',
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(ticker_id,symbol), SUBSTRING(ticker_id,symbol)",
    tags = ['gold','nado','curated']
) }}

SELECT
    hour,
    ticker_id,
    product_id,
    symbol,
    distinct_sequencer_batches,
    distinct_trader_count,
    distinct_subaccount_count,
    trade_count,
    amount_usd,
    fee_amount,
    base_delta_amount,
    quote_delta_amount,
    base_volume_24h,
    quote_volume_24h,
    funding_rate,
    index_price,
    last_price,
    mark_price,
    next_funding_rate_timestamp,
    open_interest,
    open_interest_usd,
    price_change_percent_24h,
    product_type,
    quote_currency,
    quote_volume,
    nado_market_stats_id as ez_market_stats_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__nado_market_stats') }}
{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}