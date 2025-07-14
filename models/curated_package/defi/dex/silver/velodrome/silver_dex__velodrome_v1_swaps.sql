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
        pool_name,
        regexp_substr_all(SUBSTR(DATA, 3), '.{64}') AS segmented,
        '0x' || SUBSTR(
            topics [1] :: STRING,
            27,
            40
        ) AS sender,
        '0x' || SUBSTR(
            topics [2] :: STRING,
            27,
            40
        ) AS tx_to,
        utils.udf_hex_to_int(
            segmented [0] :: STRING
        ) :: INT AS amount0In,
        utils.udf_hex_to_int(
            segmented [1] :: STRING
        ) :: INT AS amount1In,
        utils.udf_hex_to_int(
            segmented [2] :: STRING
        ) :: INT AS amount0Out,
        utils.udf_hex_to_int(
            segmented [3] :: STRING
        ) :: INT AS amount1Out,
        token0,
        token1,
        p.protocol,
        p.version,
        CONCAT(
            p.protocol,
            '-',
            p.version
        ) AS platform,
        'Swap' AS event_name,
        CONCAT(
            l.tx_hash :: STRING,
            '-',
            l.event_index :: STRING
        ) AS _log_id,
        l.modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        INNER JOIN {{ ref('silver_dex__velodrome_v1_pools') }} p
        ON l.contract_address = p.pool_address
    WHERE
        topics [0] :: STRING IN (
            '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822'
        )
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
    pool_name,
    sender,
    tx_to,
    amount0In,
    amount1In,
    amount0Out,
    amount1Out,
    token0,
    token1,
    CASE
        WHEN amount0In <> 0
        AND amount1In <> 0
        AND amount0Out <> 0 THEN amount1In
        WHEN amount0In <> 0 THEN amount0In
        WHEN amount1In <> 0 THEN amount1In
    END AS amount_in_unadj,
    CASE
        WHEN amount0Out <> 0 THEN amount0Out
        WHEN amount1Out <> 0 THEN amount1Out
    END AS amount_out_unadj,
    CASE
        WHEN amount0In <> 0
        AND amount1In <> 0
        AND amount0Out <> 0 THEN token1
        WHEN amount0In <> 0 THEN token0
        WHEN amount1In <> 0 THEN token1
    END AS token_in,
    CASE
        WHEN amount0Out <> 0 THEN token0
        WHEN amount1Out <> 0 THEN token1
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
