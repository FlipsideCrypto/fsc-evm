{% set native_token_address = get_var('GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS','') %}
{% set native_price_start_date = get_var('MAIN_CORE_NATIVE_PRICES_START_DATE','2024-01-01') %}
{% set uses_receipts_by_hash = get_var('MAIN_CORE_RECEIPTS_BY_HASH_ENABLED', false) %}
{% set gold_full_refresh = get_var('GLOBAL_GOLD_FR_ENABLED', false) %}
{% set unique_key = "tx_hash" if uses_receipts_by_hash else "block_number" %}
{% set post_hook = 'ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(origin_from_address, origin_to_address, from_address, to_address, origin_function_signature), SUBSTRING(origin_from_address, origin_to_address, from_address, to_address, origin_function_signature)' %}

{# Log configuration details #}
{{ log_model_details() }}

{% if not gold_full_refresh %}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = unique_key,
    cluster_by = ['block_timestamp::DATE'],
    incremental_predicates = [fsc_evm.standard_predicate()],
    full_refresh = gold_full_refresh,
    post_hook = post_hook,
    tags = ['gold_core', 'ez_prices_model']
) }}

{% else %}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = unique_key,
    cluster_by = ['block_timestamp::DATE'],
    incremental_predicates = [fsc_evm.standard_predicate()],
    post_hook = post_hook,
    tags = ['gold_core', 'ez_prices_model']
) }}

{% endif %}

WITH base AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        TYPE,
        trace_address,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        from_address,
        to_address,
        VALUE AS amount,
        value_precise_raw AS amount_precise_raw,
        value_precise AS amount_precise,
        ROUND(
            VALUE * price,
            2
        ) AS amount_usd,
        tx_position,
        trace_index,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'trace_index']
        ) }} AS ez_native_transfers_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp
    FROM
        {{ ref('fsc_evm', 'core__fact_traces') }}
        tr
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        ON DATE_TRUNC(
            'hour',
            block_timestamp
        ) = HOUR
        AND token_address = '{{ native_token_address }}'
    WHERE
        tr.value > 0
        AND tr.tx_succeeded
        AND tr.trace_succeeded
        AND tr.type NOT IN (
            'DELEGATECALL',
            'STATICCALL'
        )

{% if is_incremental() %}
AND tr.modified_timestamp > (
    SELECT
        COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    tx_position,
    trace_index,
    trace_address,
    TYPE,
    from_address,
    to_address,
    amount,
    amount_precise_raw,
    amount_precise,
    amount_usd,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    ez_native_transfers_id,
    inserted_timestamp,
    modified_timestamp
FROM
    base

{% if is_incremental() %}
UNION ALL
SELECT
    t.block_number,
    t.block_timestamp,
    t.tx_hash,
    t.tx_position,
    t.trace_index,
    t.trace_address,
    t.type,
    t.from_address,
    t.to_address,
    t.amount,
    t.amount_precise_raw,
    t.amount_precise,
    t.amount * p.price AS amount_usd_heal,
    t.origin_from_address,
    t.origin_to_address,
    t.origin_function_signature,
    t.ez_native_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ this }}
    t
    INNER JOIN {{ ref('price__ez_prices_hourly') }}
    p
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = HOUR
    AND token_address = '{{ native_token_address }}'
    LEFT JOIN base b USING (ez_native_transfers_id)
WHERE
    t.amount_usd IS NULL
    AND t.block_timestamp :: DATE >= '{{ native_price_start_date }}'
    AND b.ez_native_transfers_id IS NULL
{% endif %}
