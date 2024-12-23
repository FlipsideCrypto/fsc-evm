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

WITH base AS (
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    tx_position,
    event_index,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    CONCAT('0x', SUBSTR(topic_1, 27, 40)) :: STRING AS from_address,
    CONCAT('0x', SUBSTR(topic_2, 27, 40)) :: STRING AS to_address,
    utils.udf_hex_to_int(SUBSTR(DATA, 3, 64)) AS raw_amount_precise,
    raw_amount_precise::float as raw_amount,
    iff(c.decimals is null, null, utils.udf_decimal_adjust(raw_amount_precise, c.decimals)) as amount_precise,
    amount_precise::float as amount,
    iff(c.decimals is not null and price is not null, round(amount_precise * price, 2), null) as amount_usd,
    c.decimals,
    c.symbol,
    c.name,
    iff(topic_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef', 'erc20', null) as token_standard,
    fact_event_logs_id AS ez_token_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('core__fact_event_logs') }} f
    LEFT JOIN {{ ref('price__ez_prices_hourly') }} p
    ON DATE_TRUNC('hour', block_timestamp) = HOUR
    AND token_address = contract_address
    LEFT JOIN {{ ref('core__dim_contracts') }} c
    ON contract_address = c.address
    AND (c.decimals IS NOT NULL OR c.symbol IS NOT NULL OR c.name IS NOT NULL)
WHERE
    topic_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    AND tx_succeeded 
    and not event_removed
    and topic_1 is not null
    and topic_2 is not null
    and data is not null
    and raw_amount is not null

{% if is_incremental() %}
and f.modified_timestamp > (SELECT max(modified_timestamp) FROM {{ this }})
{% endif %}

)

select 
    block_number,
    block_timestamp,
    tx_hash,
    tx_position,
    event_index,
    from_address,
    to_address,
    contract_address,
    token_standard,
    name,
    symbol,
    decimals,
    raw_amount_precise,
    raw_amount,
    amount_precise,
    amount,
    amount_usd,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    ez_token_transfers_id,
    inserted_timestamp,
    modified_timestamp

from base 

{% if is_incremental() %}

union all 

select 
    t.block_number,
    t.block_timestamp,
    t.tx_hash,
    t.tx_position,
    t.event_index,
    t.from_address,
    t.to_address,
    t.contract_address,
    t.token_standard,
    c0.name,
    c0.symbol,
    c0.decimals,
    t.raw_amount_precise,
    t.raw_amount,
    iff(c0.decimals is null, null, utils.udf_decimal_adjust(t.raw_amount_precise, c0.decimals)) as amount_precise_heal,
    amount_precise_heal::float as amount_heal,
    iff(c0.decimals is not null and price is not null, round(amount_heal * price, 2), null) as amount_usd_heal,
    t.origin_function_signature,
    t.origin_from_address,
    t.origin_to_address,
    t.ez_token_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
from {{ this }} t 
left join {{ ref('core__dim_contracts') }} c0
    on t.contract_address = c0.address
    and (c0.decimals is not null or c0.symbol is not null or c0.name is not null)
left join {{ ref('price__ez_prices_hourly') }} p
    on DATE_TRUNC('hour', t.block_timestamp) = HOUR
    and t.contract_address = p.token_address
left join base b using (ez_token_transfers_id)
where (
    (t.decimals is null and c0.decimals is not null) or  -- Only heal decimals if new data exists
    (t.amount_usd is null and price is not null and t.decimals is not null) or  -- Only heal USD if we have price and decimals
    (t.symbol is null and c0.symbol is not null) or  -- Only heal symbol if new data exists
    (t.name is null and c0.name is not null)  -- Only heal name if new data exists
)
and b.ez_token_transfers_id is null
{% endif %}