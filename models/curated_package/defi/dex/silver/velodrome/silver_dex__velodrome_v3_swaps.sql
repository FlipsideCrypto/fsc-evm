{# Get variables #}
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
        l.event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        l.contract_address,
        regexp_substr_all(SUBSTR(l.data, 3, len(l.data)), '.{64}') AS l_segmented_data,
        CONCAT('0x', SUBSTR(l.topics[1]::STRING, 27, 40)) AS sender_address,
        CONCAT('0x', SUBSTR(l.topics[2]::STRING, 27, 40)) AS recipient_address,
        COALESCE(
            TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    's2c',
                    l_segmented_data[0]::STRING
                )
            ),
            0
        ) AS amount0,
        COALESCE(
            TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    's2c',
                    l_segmented_data[1]::STRING
                )
            ),
            0
        ) AS amount1,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data[2]::STRING
            )
        ) AS sqrtPriceX96,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data[3]::STRING
            )
        ) AS liquidity,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                l_segmented_data[4]::STRING
            )
        ) AS tick,
        p.token0,
        p.token1,
        p.tick_spacing,
        p.protocol,
        p.version,
        p.type,
        CONCAT(p.protocol, '-', p.version) AS platform,
        'Swap' AS event_name,
        CONCAT(l.tx_hash, '-', l.event_index) AS _log_id,
        l.modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }} l
        INNER JOIN {{ ref('silver_dex__velodrome_v3_pools') }} p
        ON l.contract_address = p.pool_address
    WHERE
        topics[0]::STRING = '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67' -- Swap (Uniswap v3 style)
        AND tx_succeeded

{% if is_incremental() %}
    AND l.modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
        FROM {{ this }}
    )
    AND l.modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    sender_address AS sender,
    recipient_address AS tx_to,
    event_index,
    amount0 AS amount0_unadj,
    amount1 AS amount1_unadj,
    sqrtPriceX96,
    liquidity,
    tick,
    tick_spacing,
    token0,
    token1,
    CASE
        WHEN amount0 > 0 THEN ABS(amount0)
        ELSE ABS(amount1)
    END AS amount_in_unadj,
    CASE
        WHEN amount0 < 0 THEN ABS(amount0)
        ELSE ABS(amount1)
    END AS amount_out_unadj,
    CASE
        WHEN amount0 > 0 THEN token0
        ELSE token1
    END AS token_in,
    CASE
        WHEN amount0 < 0 THEN token0
        ELSE token1
    END AS token_out,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp
FROM
    swaps
