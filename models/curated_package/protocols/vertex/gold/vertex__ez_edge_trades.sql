{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    unique_key = 'ez_edge_trades_id',
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(trader, symbol, subaccount), SUBSTRING(subaccount, symbol)",
    tags = ['curated', 'gold_vertex']
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    edge_event_index,
    user_event_index,
    edge_digest,
    user_digest,
    trader,
    subaccount,
    symbol,
    edge_order_type,
    user_order_type,
    edge_trade_type,
    user_trade_type,
    edge_is_taker,
    user_is_taker,
    edge_price_amount_unadj,
    user_price_amount_unadj,
    edge_price_amount,
    user_price_amount,
    edge_amount_unadj,
    user_amount_unadj,
    edge_amount,
    user_amount,
    edge_amount_usd,
    user_amount_usd,
    edge_fee_amount_unadj,
    user_fee_amount_unadj,
    edge_fee_amount,
    user_fee_amount,
    edge_base_delta_amount_unadj,
    user_base_delta_amount_unadj,
    edge_base_delta_amount,
    user_base_delta_amount,
    edge_quote_delta_amount_unadj,
    user_quote_delta_amount_unadj,
    edge_quote_delta_amount,
    user_quote_delta_amount,
    vertex_edge_trade_id as ez_edge_trades_id,
    inserted_timestamp,
    modified_timestamp,
FROM
    {{ ref('silver__vertex_edge_trades') }}
{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}