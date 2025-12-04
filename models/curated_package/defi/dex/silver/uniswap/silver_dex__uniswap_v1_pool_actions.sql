{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
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
        token0,
        token1,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        data,
        CASE 
            WHEN topic_0 = '0x06239653922ac7bea6aa2b19dc486b9361821d37712eb796adfd38d81de278ca' THEN 'AddLiquidity'
            ELSE 'RemoveLiquidity'
        END AS event_name,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS provider_address,
        utils.udf_hex_to_int(
            topic_2
        ) :: FLOAT AS eth_amount,
        utils.udf_hex_to_int(
            topic_3
        ) :: FLOAT AS token_amount,
        provider_address AS liquidity_provider,
        provider_address AS sender,
        CASE
            WHEN event_name = 'AddLiquidity' THEN pool_address
            ELSE provider_address
        END AS receiver,
        p.protocol,
        p.version,
        p.type,
        CONCAT(
            p.protocol,
            '-',
            p.version
        ) AS platform,
        CONCAT(
            l.tx_hash :: STRING,
            '-',
            l.event_index :: STRING
        ) AS _log_id,
        l.modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        INNER JOIN {{ref('silver_dex__uniswap_v1_pools')}} p
        ON l.contract_address = p.pool_address
    WHERE
        topic_0 IN ('0x06239653922ac7bea6aa2b19dc486b9361821d37712eb796adfd38d81de278ca', --AddLiquidity
        '0x0fbf06c058b90cb038a618f8c2acbf6145f8b3570fd1fa56abb8f0f3f05b36e8' --RemoveLiquidity
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
    token_amount AS amount0_unadj,
    eth_amount AS amount1_unadj,
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
