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
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        block_timestamp,
        l.tx_hash,
        l.event_index,
        l.contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS amountIn,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS amountOut,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS tx_to,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS tokenIn,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS tokenOut,
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
        INNER JOIN {{ ref('silver_dex__sushi_pools') }} p
        ON l.contract_address = p.pool_address
    WHERE
        topics [0] :: STRING = '0xcd3829a3813dc3cdd188fd3d01dcf3268c16be2fdd2dd21d0665418816e46062'
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
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    event_name,
    contract_address,
    origin_from_address AS sender,
    tx_to,
    amountIn AS amount_in_unadj,
    amountOut AS amount_out_unadj,
    tokenIn AS token_in,
    tokenOut AS token_out,
    token0,
    token1,
    platform,
    protocol,
    version,
    _log_id,
    modified_timestamp
FROM
    swaps