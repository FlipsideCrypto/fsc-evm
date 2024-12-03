{{ config(
    materialized = 'incremental',
    unique_key = 'ez_liquidations_id',
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(trader, subaccount,digest), SUBSTRING(subaccount,trader)",
    tags = ['curated', 'gold_vertex']
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
    digest,
    trader,
    subaccount,
    product_id,
    health_group,
    health_group_symbol,
    amount_unadj,
    amount,
    amount_quote_unadj,
    amount_quote,
    is_encoded_spread,
    spread_product_ids,
    vertex_liquidation_id AS ez_liquidations_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__vertex_liquidations') }}
{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}