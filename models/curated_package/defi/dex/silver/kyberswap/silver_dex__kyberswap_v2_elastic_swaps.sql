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
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        l.event_index,
        l.contract_address,
        regexp_substr_all(SUBSTR(l.data, 3, len(l.data)), '.{64}') AS l_segmented_data,
        CONCAT('0x', SUBSTR(l.topics [1] :: STRING, 27, 40)) AS sender_address,
        CONCAT('0x', SUBSTR(l.topics [2] :: STRING, 27, 40)) AS reipient_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                l_segmented_data [0] :: STRING
            )
        ) AS deltaQty0,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                l_segmented_data [1] :: STRING
            )
        ) AS deltaQty1,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [2] :: STRING
            )
        ) AS sqrtP,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [3] :: STRING
            )
        ) AS liquidity,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                l_segmented_data [4] :: STRING
            )
        ) AS currentTick,
        ABS(GREATEST(deltaQty0, deltaQty1)) AS amountOut,
        ABS(LEAST(deltaQty0, deltaQty1)) AS amountIn,
        token0,
        token1,
        CASE
            WHEN deltaQty0 < 0 THEN token0
            ELSE token1
        END AS token_in,
        CASE
            WHEN deltaQty0 > 0 THEN token0
            ELSE token1
        END AS token_out,
        p.platform,
        p.protocol,
        p.version,
        p.type,
        'ElasticSwap' AS event_name,
        CONCAT(
            l.tx_hash :: STRING,
            '-',
            l.event_index :: STRING
        ) AS _log_id,
        l.modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        INNER JOIN {{ ref('silver_dex__kyberswap_v2_elastic_pools') }} p
        ON p.pool_address = l.contract_address
    WHERE
        topics [0] :: STRING = '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67' -- elastic swap
        AND tx_succeeded

{% if is_incremental() %}
AND l.modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
AND l.modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    event_index,
    event_name,
    sender_address AS sender,
    reipient_address AS tx_to,
    deltaQty0 AS delta_qty0,
    deltaQty1 AS delta_qty1,
    sqrtP AS sqrt_p,
    liquidity,
    currentTick AS current_tick,
    amountIn AS amount_in_unadj,
    amountOut AS amount_out_unadj,
    token0,
    token1,
    token_in,
    token_out,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp
FROM
    swaps
WHERE
    token_in <> token_out
