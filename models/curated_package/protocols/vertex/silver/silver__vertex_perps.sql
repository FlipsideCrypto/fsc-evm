{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'fact_event_logs_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','curated','vertex']
) }}


WITH vertex_products AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        product_id :: STRING AS product_id,
        product_type,
        ticker_id,
        symbol,
        NAME
    FROM
        {{ ref('silver__vertex_dim_products') }}
    WHERE
        product_type = 'perp'
),
logs AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_succeeded,
        contract_address,
        data,
        event_index,
        event_removed,
        topics,
        fact_event_logs_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING = '0x7c57459d6f4f0fb2fc5b1e298c8c0eb238422944964aa1e249eaa78747f0cca9'
        AND contract_address = '{{ vars.CURATED_VERTEX_OFFCHAIN_EXCHANGE_CONTRACT }}'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(
            modified_timestamp
        ) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
{% endif %}
),
order_fill_decode_v2 AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        'FillOrder' AS event_name,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        utils.udf_hex_to_int(
            topics [1] :: STRING
        ) :: STRING AS product_id,
        topics [2] :: STRING AS digest,
        --unique hash of the order
        LEFT(
            topics [3] :: STRING,
            42
        ) AS trader,
        topics [3] :: STRING AS subaccount,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INT AS pricex18,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [1] :: STRING
        ) :: INT AS amount,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) AS expiration,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: INT AS nonce,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [4] :: STRING
        ) :: INT AS isTaker,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [5] :: STRING
        ) :: INT AS feeAmount,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [6] :: STRING
        ) :: INT AS baseDelta,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [7] :: STRING
        ) :: INT AS quoteDelta,
        l.fact_event_logs_id,
        l.modified_timestamp
    FROM
        logs l
),
order_fill_format AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        l.contract_address,
        event_name,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        s.symbol,
        l.product_id,
        digest,
        trader,
        subaccount,
        expiration AS expiration_raw,
        utils.udf_int_to_binary(TRY_TO_NUMBER(expiration)) AS exp_binary,
        utils.udf_binary_to_int(SUBSTR(exp_binary, -2)) AS order_type,
        utils.udf_binary_to_int(SUBSTR(exp_binary, -3, 1)) AS market_reduce_flag,
        CASE
            WHEN len(expiration) < 11 THEN TRY_TO_TIMESTAMP(utils.udf_binary_to_int(exp_binary) :: STRING)
            ELSE TRY_TO_TIMESTAMP(
                utils.udf_binary_to_int(SUBSTR(exp_binary, 24)) :: STRING
            )
        END AS expiration,
        nonce,
        isTaker,
        feeAmount AS fee_amount_unadj,
        feeAmount / pow(
            10,
            18
        ) AS fee_amount,
        pricex18 AS price_amount_unadj,
        pricex18 / pow(
            10,
            18
        ) AS price_amount,
        amount AS amount_unadj,
        amount / pow(
            10,
            18
        ) AS amount,
        baseDelta AS base_delta_unadj,
        baseDelta / pow(
            10,
            18
        ) AS base_delta,
        quoteDelta AS quote_delta_unadj,
        quoteDelta / pow(
            10,
            18
        ) AS quote_delta,
        l.fact_event_logs_id,
        l.modified_timestamp
    FROM
        order_fill_decode_v2 l
    INNER JOIN vertex_products s
    ON s.product_id = l.product_id
),
FINAL AS (
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
        product_id,
        digest,
        trader,
        subaccount,
        CASE
            WHEN amount < 0 THEN 'sell/short'
            WHEN amount > 0 THEN 'buy/long'
        END AS trade_type,
        expiration_raw,
        exp_binary,
        order_type AS order_type_raw,
        CASE
            WHEN order_type = 0 THEN 'default'
            WHEN order_type = 1 THEN 'immediate-or-cancel'
            WHEN order_type = 2 THEN 'fill-or-kill'
            WHEN order_type = 3 THEN 'post-only'
        END AS order_type,
        market_reduce_flag,
        expiration,
        nonce,
        CASE
            WHEN isTaker = 1 THEN TRUE
            WHEN isTaker = 0 THEN FALSE
        END AS is_taker,
        price_amount_unadj,
        price_amount,
        amount_unadj,
        amount,
        CASE
            WHEN quote_delta < 0 THEN quote_delta * -1
            ELSE quote_delta
        END AS amount_usd,
        fee_amount_unadj,
        fee_amount,
        base_delta_unadj AS base_delta_amount_unadj,
        base_delta AS base_delta_amount,
        quote_delta_unadj AS quote_delta_amount_unadj,
        quote_delta AS quote_delta_amount,
        fact_event_logs_id,
        modified_timestamp
    FROM
        order_fill_format
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS vertex_perps_id,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY fact_event_logs_id
ORDER BY
    modified_timestamp DESC)) = 1
