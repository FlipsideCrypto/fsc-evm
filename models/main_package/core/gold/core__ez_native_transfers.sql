{% set native_token_address = var('GLOBAL_WRAPPED_ASSET_ADDRESS','') %}
{% set native_price_start_date = var('NATIVE_PRICE_START_DATE','2024-01-01') %}
{% set uses_receipts_by_hash = var('GLOBAL_USES_RECEIPTS_BY_HASH', false) %}
{% set gold_full_refresh = var('GOLD_FULL_REFRESH', false) %}
{% set unique_key = "tx_hash" if uses_receipts_by_hash else "block_number" %}
{% set post_hook = 'ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(origin_from_address, origin_to_address, from_address, to_address, origin_function_signature), SUBSTRING(origin_from_address, origin_to_address, from_address, to_address, origin_function_signature)' %}

{% if not gold_full_refresh %}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = unique_key,
    cluster_by = ['block_timestamp::DATE'],
    incremental_predicates = [fsc_evm.standard_predicate()],
    full_refresh = gold_full_refresh,
    post_hook = post_hook,
    tags = ['gold_core']
) }}

{% else %}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = unique_key,
    cluster_by = ['block_timestamp::DATE'],
    incremental_predicates = [fsc_evm.standard_predicate()],
    post_hook = post_hook,
    tags = ['gold_core']
) }}

{% endif %}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    type,
    trace_address,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    from_address,
    to_address,
    value AS amount,
    value_precise_raw AS amount_precise_raw,
    value_precise AS amount_precise,
    ROUND(
        value * price,
        2
    ) AS amount_usd,
    tx_position,
    trace_index,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'trace_index']
    ) }} AS native_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('core__fact_traces') }} tr
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = HOUR
    AND token_address = '{{ native_token_address }}'

{% if is_incremental() %}
where tr.modified_timestamp > (SELECT max(modified_timestamp) FROM {{ this }})

union all 

select 
    t.block_number,
    t.block_timestamp,
    t.tx_hash,
    t.type,
    t.trace_address,
    t.from_address,
    t.to_address,
    t.amount,
    t.amount_precise_raw,
    t.amount_precise,
    t.amount * p.price as amount_usd_heal,
    t.tx_position,
    t.trace_index,
    t.native_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
from {{ this }} t
inner join {{ ref('price__ez_prices_hourly') }} p
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = HOUR
    and token_address = '{{ native_token_address }}'
where t.amount_usd is null
and t.modified_timestamp > current_date() - 30
and t.block_timestamp::date >= '{{ native_price_start_date }}'

{% endif %}