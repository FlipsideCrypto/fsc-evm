{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    unique_key = 'ez_spot_trades_id',
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,symbol,trader,digest,subaccount), SUBSTRING(symbol,trader)",
    tags = get_path_tags(model)
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    event_name,
    event_index,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    symbol,
    digest,
    trader,
    subaccount,
    trade_type,
    order_type,
    market_reduce_flag,
    expiration,
    nonce,
    is_taker,
    price_amount_unadj,
    price_amount,
    amount_unadj,
    amount,
    amount_usd,
    fee_amount_unadj,
    fee_amount,
    base_delta_amount_unadj,
    base_delta_amount,
    quote_delta_amount_unadj,
    quote_delta_amount,
    vertex_spot_id AS ez_spot_trades_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__vertex_spot') }}
{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}