{% set vars = return_vars() %}
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
        'Swap' AS event_name,
        -- Parse swap data: swap0to1 (bool) | amountIn | amountOut | to
        CASE
            WHEN SUBSTR(data, 3, 64) = '0000000000000000000000000000000000000000000000000000000000000001'
            THEN TRUE
            ELSE FALSE
        END AS swap0to1,
        utils.udf_hex_to_int(SUBSTR(data, 67, 64)) :: FLOAT AS amount_in,
        utils.udf_hex_to_int(SUBSTR(data, 131, 64)) :: FLOAT AS amount_out,
        CONCAT('0x', SUBSTR(data, 219, 40)) AS recipient,
        -- Token direction based on swap0to1:
        -- swap0to1 = true: selling token0 for token1
        -- swap0to1 = false: selling token1 for token0
        CASE
            WHEN SUBSTR(data, 3, 64) = '0000000000000000000000000000000000000000000000000000000000000001'
            THEN p.token0  -- swap0to1 = true, token_in is token0
            ELSE p.token1  -- swap0to1 = false, token_in is token1
        END AS token_in,
        CASE
            WHEN SUBSTR(data, 3, 64) = '0000000000000000000000000000000000000000000000000000000000000001'
            THEN p.token1  -- swap0to1 = true, token_out is token1
            ELSE p.token0  -- swap0to1 = false, token_out is token0
        END AS token_out,
        p.platform,
        p.protocol,
        p.version,
        p.type,
        CONCAT(
            l.tx_hash :: STRING,
            '-',
            l.event_index :: STRING
        ) AS _log_id,
        l.modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }} l
        INNER JOIN {{ ref('silver_dex__fluid_v1_pools') }} p
        ON l.contract_address = p.pool_address
    WHERE
        topics [0] :: STRING = '0xdc004dbca4ef9c966218431ee5d9133d337ad018dd5b5c5493722803f75c64f7' -- Swap
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
    event_index,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    recipient AS sender,
    recipient AS tx_to,
    amount_in AS amount_in_unadj,
    amount_out AS amount_out_unadj,
    token_in,
    token_out,
    platform,
    protocol,
    version,
    type,
    event_name,
    _log_id,
    modified_timestamp
FROM
    swaps
