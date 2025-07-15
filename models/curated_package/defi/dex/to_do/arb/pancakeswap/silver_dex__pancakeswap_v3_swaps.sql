{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_dex','defi','dex','curated']
) }}

WITH pools AS (

    SELECT
        pool_address,
        token0_address,
        token1_address,
        fee,
        tick_spacing
    FROM
        {{ ref('silver_dex__pancakeswap_v3_pools') }}
),
base_swaps AS (
    SELECT
        l.block_number,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        l.block_timestamp,
        l.tx_hash,
        l.event_index,
        l.contract_address,
        regexp_substr_all(SUBSTR(l.data, 3, len(l.data)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(l.topics [1] :: STRING, 27, 40)) AS sender_address,
        CONCAT('0x', SUBSTR(l.topics [2] :: STRING, 27, 40)) AS recipient_address,
        COALESCE(
            TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    's2c',
                    segmented_data [0] :: STRING
                )
            ),
            0
        ) AS amount0,
        COALESCE(
            TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    's2c',
                    segmented_data [1] :: STRING
                )
            ),
            0
        ) AS amount1,
        COALESCE(
            TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    segmented_data [2] :: STRING
                )
            ),
            0
        ) AS sqrtPriceX96,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [3] :: STRING
            )
        ) AS liquidity,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [4] :: STRING
            )
        ) AS tick,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [5] :: STRING
            )
        ) AS protocolFeesToken0,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [6] :: STRING
            )
        ) AS protocolFeesToken1,
        ABS(LEAST(amount0, amount1)) AS amount_out_unadj,
        ABS(GREATEST(amount0, amount1)) AS amount_in_unadj,
        token0_address,
        token1_address,
        CASE
            WHEN amount0 > 0 THEN token0_address
            ELSE token1_address
        END AS token_in,
        CASE
            WHEN amount1 < 0 THEN token1_address
            ELSE token0_address
        END AS token_out,
        fee,
        tick_spacing,
        CONCAT(
            l.tx_hash,
            '-',
            l.event_index
        ) AS _log_id,
        l.modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        INNER JOIN pools p
        ON l.contract_address = pool_address
    WHERE
        block_timestamp :: DATE > '2023-06-01'
        AND topic_0 = '0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83' --swap
        AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    'pancakeswap-v3' AS platform,
    'v3' AS version,
    contract_address,
    contract_address AS pool_address,
    sender_address AS sender,
    recipient_address AS tx_to,
    event_index,
    'Swap' AS event_name,
    amount0,
    amount1,
    sqrtPriceX96,
    liquidity,
    tick,
    tick_spacing,
    fee,
    protocolFeesToken0,
    protocolFeesToken1,
    amount_out_unadj,
    amount_in_unadj,
    token0_address,
    token1_address,
    token_in,
    token_out,
    _log_id,
    modified_timestamp
FROM
    base_swaps
