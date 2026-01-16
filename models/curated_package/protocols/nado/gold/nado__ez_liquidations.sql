{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    unique_key = 'ez_liquidations_id',
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(trader, subaccount,digest), SUBSTRING(subaccount,trader)",
    tags = ['gold','nado','curated']
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
    nado_liquidation_id AS ez_liquidations_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__nado_liquidations') }}
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