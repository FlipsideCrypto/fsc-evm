{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = vars.MAIN_CORE_GOLD_EZ_NATIVE_TRANSFERS_UNIQUE_KEY,
    cluster_by = ['block_timestamp::DATE'],
    incremental_predicates = [fsc_evm.standard_predicate()],
    full_refresh = vars.GLOBAL_GOLD_FR_ENABLED,
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(origin_from_address, origin_to_address, from_address, to_address, origin_function_signature), SUBSTRING(origin_from_address, origin_to_address, from_address, to_address, origin_function_signature)",
    tags = ['gold','core','transfers','ez','phase_3']
) }}

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
            VALUE * COALESCE(p0.price, p1.price),
            2
        ) AS amount_usd,
        tx_position,
        trace_index,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'trace_index']
        ) }} AS ez_native_transfers_id,
        {% if is_incremental() or vars.GLOBAL_NEW_BUILD_ENABLED %}
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp
        {% else %}
        CASE WHEN block_timestamp >= date_trunc('hour',SYSDATE()) - interval '6 hours' THEN SYSDATE() 
            ELSE GREATEST(block_timestamp, dateadd('day', -10, SYSDATE())) END AS inserted_timestamp,
        CASE WHEN block_timestamp >= date_trunc('hour',SYSDATE()) - interval '6 hours' THEN SYSDATE() 
            ELSE GREATEST(block_timestamp, dateadd('day', -10, SYSDATE())) END AS modified_timestamp
        {% endif %}
    FROM
        {{ ref('core__fact_traces') }}
        tr
        LEFT JOIN {{ ref('price__ez_prices_hourly') }} p0
        ON DATE_TRUNC(
            'hour',
            block_timestamp
        ) = p0.HOUR
        AND p0.token_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}'
        LEFT JOIN {{ ref('price__ez_prices_hourly') }} p1
        ON DATE_TRUNC(
            'hour',
            block_timestamp
        ) = p1.HOUR
        and p1.is_native
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
),
final AS (
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
    t.amount * COALESCE(p0.price, p1.price) AS amount_usd_heal,
    t.origin_from_address,
    t.origin_to_address,
    t.origin_function_signature,
    t.ez_native_transfers_id,
    {% if is_incremental() or vars.GLOBAL_NEW_BUILD_ENABLED %}
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
    {% else %}
    CASE WHEN t.block_timestamp >= date_trunc('hour',SYSDATE()) - interval '6 hours' THEN SYSDATE() 
        ELSE GREATEST(t.block_timestamp, dateadd('day', -10, SYSDATE())) END AS inserted_timestamp,
    CASE WHEN t.block_timestamp >= date_trunc('hour',SYSDATE()) - interval '6 hours' THEN SYSDATE() 
        ELSE GREATEST(t.block_timestamp, dateadd('day', -10, SYSDATE())) END AS modified_timestamp
    {% endif %}
FROM
    {{ this }}
    t
    LEFT JOIN {{ ref('price__ez_prices_hourly') }} p0
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = p0.HOUR
    AND p0.token_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}'
    LEFT JOIN {{ ref('price__ez_prices_hourly') }} p1
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = p1.HOUR
    and p1.is_native
    LEFT JOIN base b USING (ez_native_transfers_id)
WHERE
    t.amount_usd IS NULL
    AND t.block_timestamp :: DATE >= '{{ vars.MAIN_CORE_GOLD_EZ_NATIVE_TRANSFERS_PRICES_START_DATE }}'
    AND b.ez_native_transfers_id IS NULL
    and COALESCE(p0.price, p1.price) is not null
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
    final

qualify(ROW_NUMBER() over(PARTITION BY ez_native_transfers_id
ORDER BY
    modified_timestamp DESC)) = 1
