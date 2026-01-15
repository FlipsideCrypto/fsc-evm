{% set vars = return_vars() %}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_dex','defi','dex','lp_actions','curated']
) }}

WITH evt AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        l.event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        l.contract_address AS pool_address,
        p.token0,
        p.token1,
        topics[0]::STRING AS topic_0,
        CASE
            WHEN topics[0]::STRING IN (
                '0xbfea92097a2487d6a5ccf7b7adc36b6002238f3106568ba4359770f4b67365a4', -- LogDepositColLiquidity
                '0x255672effa3d8ba46e409fc964ae332b84d3107ba3a5096b22734606519528a3'  -- LogDepositPerfectColLiquidity
            ) THEN 'AddLiquidity'
            ELSE 'RemoveLiquidity'
        END AS event_name,
        origin_from_address AS liquidity_provider,
        origin_from_address AS sender,
        CASE
            WHEN topics[0]::STRING IN (
                '0xbfea92097a2487d6a5ccf7b7adc36b6002238f3106568ba4359770f4b67365a4',
                '0x255672effa3d8ba46e409fc964ae332b84d3107ba3a5096b22734606519528a3'
            ) THEN pool_address
            ELSE origin_from_address
        END AS receiver,
        -- Amount parsing depends on event type
        -- For regular deposit/withdraw: shares at offset 65, token0 at 1, token1 at 33
        -- For perfect deposit/withdraw: shares at offset 1, token0 at 33, token1 at 65
        CASE
            WHEN topics[0]::STRING IN (
                '0xbfea92097a2487d6a5ccf7b7adc36b6002238f3106568ba4359770f4b67365a4', -- LogDepositColLiquidity
                '0xb61c7f3b23fe9335cc6c6a6e7036457758470877e61a19a5b4924e1ff8289624'  -- LogWithdrawColLiquidity
            ) THEN utils.udf_hex_to_int(SUBSTR(data, 3, 64))::FLOAT
            ELSE utils.udf_hex_to_int(SUBSTR(data, 67, 64))::FLOAT
        END AS amount0_raw,
        CASE
            WHEN topics[0]::STRING IN (
                '0xbfea92097a2487d6a5ccf7b7adc36b6002238f3106568ba4359770f4b67365a4', -- LogDepositColLiquidity
                '0xb61c7f3b23fe9335cc6c6a6e7036457758470877e61a19a5b4924e1ff8289624'  -- LogWithdrawColLiquidity
            ) THEN utils.udf_hex_to_int(SUBSTR(data, 67, 64))::FLOAT
            ELSE utils.udf_hex_to_int(SUBSTR(data, 131, 64))::FLOAT
        END AS amount1_raw,
        p.protocol,
        p.version,
        p.type,
        CONCAT(
            p.protocol,
            '-',
            p.version
        ) AS platform,
        CONCAT(
            l.tx_hash::STRING,
            '-',
            l.event_index::STRING
        ) AS _log_id,
        l.modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }} l
        INNER JOIN {{ ref('silver_dex__fluid_v1_pools') }} p
        ON l.contract_address = p.pool_address
    WHERE
        topics[0]::STRING IN (
            '0xbfea92097a2487d6a5ccf7b7adc36b6002238f3106568ba4359770f4b67365a4', -- LogDepositColLiquidity
            '0x255672effa3d8ba46e409fc964ae332b84d3107ba3a5096b22734606519528a3', -- LogDepositPerfectColLiquidity
            '0xb61c7f3b23fe9335cc6c6a6e7036457758470877e61a19a5b4924e1ff8289624', -- LogWithdrawColLiquidity
            '0x6f837572c1ef6e010a841ff938d593ec054984fefe29df2a0634bbf01f4db35b', -- LogWithdrawPerfectColLiquidity
            '0xc98f37914e06db36c18654484db85c4bb864575a1b9f8181133ff33dea2d34f3'  -- LogWithdrawColInOneToken
        )
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
    event_name,
    pool_address,
    token0,
    token1,
    liquidity_provider,
    sender,
    receiver,
    amount0_raw AS amount0_unadj,
    amount1_raw AS amount1_unadj,
    protocol,
    version,
    type,
    platform,
    _log_id,
    modified_timestamp
FROM
    evt qualify(ROW_NUMBER() over (PARTITION BY _log_id
ORDER BY
    modified_timestamp DESC)) = 1
