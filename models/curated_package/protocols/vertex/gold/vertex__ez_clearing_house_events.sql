{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    unique_key = 'ez_clearing_house_events_id',
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(trader, symbol, subaccount), SUBSTRING(subaccount, symbol)",
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
    modification_type,
    symbol,
    trader,
    subaccount,
    token_address,
    amount_unadj,
    amount,
    amount_usd,
    vertex_collateral_id AS ez_clearing_house_events_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__vertex_collateral') }}
{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}