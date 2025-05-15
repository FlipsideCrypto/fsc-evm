{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = vars.MAIN_CORE_GOLD_EZ_TOKEN_TRANSFERS_UNIQUE_KEY,
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
        tx_position,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) :: STRING AS from_address,
        CONCAT('0x', SUBSTR(topic_2, 27, 40)) :: STRING AS to_address,
        utils.udf_hex_to_int(SUBSTR(DATA, 3, 64)) AS raw_amount_precise,
        raw_amount_precise :: FLOAT AS raw_amount,
        IFF(
            C.decimals IS NULL,
            NULL,
            utils.udf_decimal_adjust(
                raw_amount_precise,
                C.decimals
            )
        ) AS amount_precise,
        amount_precise :: FLOAT AS amount,
        IFF(
            C.decimals IS NOT NULL
            AND price IS NOT NULL,
            ROUND(
                amount_precise * price,
                2
            ),
            NULL
        ) AS amount_usd,
        C.decimals,
        C.symbol,
        C.name,
        IFF(
            topic_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
            'erc20',
            NULL
        ) AS token_standard,
        fact_event_logs_id AS ez_token_transfers_id,
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
        {{ ref('core__fact_event_logs') }}
        f
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        p
        ON DATE_TRUNC(
            'hour',
            block_timestamp
        ) = HOUR
        AND token_address = contract_address
        LEFT JOIN {{ ref('core__dim_contracts') }} C
        ON contract_address = C.address
        AND (
            C.decimals IS NOT NULL
            OR C.symbol IS NOT NULL
            OR C.name IS NOT NULL
        )
    WHERE
        topic_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        AND tx_succeeded
        AND NOT event_removed
        AND topic_1 IS NOT NULL
        AND topic_2 IS NOT NULL
        AND DATA IS NOT NULL
        AND raw_amount IS NOT NULL

{% if is_incremental() %}
AND f.modified_timestamp > (
    SELECT
        COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
    FROM
        {{ this }}
)
{% endif %}
),
{% if is_incremental() %}
recent_token_transfers as (
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
        amount_usd,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        ez_token_transfers_id
    from {{ this }}
    where block_timestamp >= dateadd('day', -31, SYSDATE())
),
heal_token_transfers as (
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
        c.name,
        c.symbol,
        c.decimals,
        r.raw_amount_precise,
        r.raw_amount,
        IFF(
            c.decimals IS NULL,
            NULL,
            utils.udf_decimal_adjust(
                r.raw_amount_precise,
                c.decimals
            )
        ) AS amount_precise,
        amount_precise :: FLOAT AS amount,
        IFF(
            c.decimals IS NOT NULL
            AND p.price IS NOT NULL,
            ROUND(
                amount * p.price,
                2
            ),
            NULL
        ) AS amount_usd,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        ez_token_transfers_id
    from recent_token_transfers r 
    left join {{ ref('core__dim_contracts') }} c
    on r.contract_address = c.address
    left join {{ ref('price__ez_prices_hourly') }} p
    on DATE_TRUNC('hour', r.block_timestamp) = p.hour
    and r.contract_address = p.token_address
    where (r.decimals is null or r.amount_usd is null)
    and (c.decimals is not null or p.price is not null)
),
{% endif %}
final AS (
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    tx_position,
    event_index,
    from_address,
    to_address,
    contract_address,
    token_standard,
    NAME,
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
FROM
    base

{% if is_incremental() %}
UNION ALL
SELECT
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
    amount_usd,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    ez_token_transfers_id,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp
FROM recent_token_transfers
where block_number in (select block_number from heal_token_transfers)
UNION ALL
SELECT
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
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp
from heal_token_transfers
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    tx_position,
    event_index,
    from_address,
    to_address,
    contract_address,
    token_standard,
    NAME,
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
FROM
    final
{% if is_incremental() %}
qualify(ROW_NUMBER() over(PARTITION BY ez_token_transfers_id
    ORDER BY modified_timestamp DESC, amount_usd DESC NULLS LAST)) = 1
{% endif %}
