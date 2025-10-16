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
        tokenX AS token0,
        tokenY AS token1,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        decoded_log :amountX :: FLOAT AS amount0,
        decoded_log :amountY :: FLOAT AS amount1,
        decoded_log :id :: STRING AS id,
        decoded_log :sender :: STRING AS sender_address,
        decoded_log :recipient :: STRING AS receiver_address,
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
        {{ ref('core__ez_decoded_event_logs') }}
        l
        INNER JOIN {{ref('silver_dex__trader_joe_v2_pools')}} p
        ON l.contract_address = p.lb_pair
    WHERE
        topic_0 IN ('0x4216cc3bd0c40a90259d92f800c06ede5c47765f41a488072b7e7104a1f95841', --DepositedToBin
        '0xda5e7177dface55f5e0eff7dfc67420a1db4243ddfcf0ecc84ed93e034dd8cc2' --WithdrawnFromBin
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
),
deposit AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        'DepositedToBin' AS event_name,
        pool_address,
        token0,
        token1,
        sender_address,
        receiver_address,
        amount0,
        amount1,
        protocol,
        version,
        type,
        platform,
        _log_id,
        modified_timestamp
    FROM
        evt
    WHERE
        topic_0 = '0x4216cc3bd0c40a90259d92f800c06ede5c47765f41a488072b7e7104a1f95841' --mint
),
withdraw AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        'WithdrawnFromBin' AS event_name,
        pool_address,
        token0,
        token1,
        sender_address,
        receiver_address,
        amount0,
        amount1,
        protocol,
        version,
        type,
        platform,
        _log_id,
        modified_timestamp
    FROM
        evt
    WHERE
        topic_0 = '0xda5e7177dface55f5e0eff7dfc67420a1db4243ddfcf0ecc84ed93e034dd8cc2' --burn
),
all_actions AS (
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
        sender_address AS sender,
        receiver_address AS receiver,
        amount0 AS amount0_unadj,
        amount1 AS amount1_unadj,
        protocol,
        version,
        type,
        platform,
        _log_id,
        modified_timestamp
    FROM
        deposit
    UNION ALL
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
        sender_address AS sender,
        receiver_address AS receiver,
        amount0 AS amount0_unadj,
        amount1 AS amount1_unadj,
        protocol,
        version,
        type,
        platform,
        _log_id,
        modified_timestamp
    FROM
        withdraw
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
    origin_from_address AS liquidity_provider,
    sender,
    receiver,
    amount0_unadj,
    amount1_unadj,
    protocol,
    version,
    type,
    platform,
    _log_id,
    modified_timestamp
FROM
    all_actions qualify(ROW_NUMBER() over (PARTITION BY _log_id
ORDER BY
    modified_timestamp DESC)) = 1
