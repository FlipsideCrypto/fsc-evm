{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_dex','defi','dex','curated']
) }}

WITH swaps AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        l.event_index,
        l.contract_address,
        regexp_substr_all(SUBSTR(l.data, 3, len(l.data)), '.{64}') AS l_segmented_data,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [0] :: STRING,
                25,
                40
            )
        ) AS sender_address,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [1] :: STRING,
                25,
                40
            )
        ) AS recipient_address,
        CASE
            WHEN TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    l_segmented_data [3] :: STRING
                )
            ) = 0 THEN FALSE
            ELSE TRUE
        END AS tokenAin,
        CASE
            WHEN TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    l_segmented_data [4] :: STRING
                )
            ) = 0 THEN FALSE
            ELSE TRUE
        END AS exactOutput,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                l_segmented_data [5] :: STRING
            )
        ) AS activetick,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [6] :: STRING
            )
        ) AS amountIn,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [7] :: STRING
            )
        ) AS amountOut,
        tokenA,
        tokenB,
        p.platform,
        p.protocol,
        p.version,
        'Swap' AS event_name,
        CONCAT(
            l.tx_hash,
            '-',
            l.event_index
        ) AS _log_id,
        l.modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        INNER JOIN {{ ref('silver_dex__maverick_v2_pools') }} p
        ON l.contract_address = p.pool_address
    WHERE
        l.topic_0 :: STRING = '0x103ed084e94a44c8f5f6ba8e3011507c41063177e29949083c439777d8d63f60' --Swap
        AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    event_index,
    event_name,
    contract_address,
    contract_address AS pool_address,
    sender_address AS sender,
    recipient_address AS tx_to,
    tokenAin AS token_A_in,
    exactOutput AS exact_output,
    amountIn AS amount_in_unadj,
    amountOut AS amount_out_unadj,
    activetick,
    CASE
        WHEN token_A_in = TRUE THEN tokenA
        ELSE tokenB
    END AS token_in,
    CASE
        WHEN token_A_in = TRUE THEN tokenB
        ELSE tokenA
    END AS token_out,
    platform,
    protocol,
    version,
    _log_id,
    modified_timestamp
FROM
    swaps
WHERE
    token_in <> token_out
