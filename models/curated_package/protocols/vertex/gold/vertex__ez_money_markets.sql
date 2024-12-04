{{ config(
    materialized = 'incremental',
    unique_key = 'ez_money_markets_id',
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(ticker_id,product_id,symbol), SUBSTRING(ticker_id,product_id,symbol)",
    tags = ['curated', 'gold_vertex']
) }}

SELECT
    hour,
    ticker_id,
    symbol,
    product_id,
    deposit_apr,
    borrow_apr,
    tvl,
    vertex_money_markets_id as ez_money_markets_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__vertex_money_markets') }}
{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}