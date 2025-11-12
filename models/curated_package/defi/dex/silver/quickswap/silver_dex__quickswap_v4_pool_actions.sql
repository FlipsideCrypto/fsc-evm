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
        token0_address AS token0,
        token1_address AS token1,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        DATA,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
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
        INNER JOIN {{ ref('silver_dex__quickswap_v4_pools') }}
        p
        ON l.contract_address = p.pool_address
    WHERE
        topic_0 IN (
            '0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde',
            --mint
            '0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c' --burn
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
mint AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        'Mint' AS event_name,
        pool_address,
        token0,
        token1,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS owner_address,
        utils.udf_hex_to_int(
            topic_2
        ) :: FLOAT AS bottom_tick,
        utils.udf_hex_to_int(
            topic_3
        ) :: FLOAT AS top_tick,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS sender_address,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: FLOAT AS liquidity_amount,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: FLOAT AS amount0,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: FLOAT AS amount1,
        protocol,
        version,
        TYPE,
        platform,
        _log_id,
        modified_timestamp
    FROM
        evt
    WHERE
        topic_0 = '0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde' --mint
),
burn AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        'Burn' AS event_name,
        pool_address,
        token0,
        token1,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS owner_address,
        utils.udf_hex_to_int(
            topic_2
        ) :: FLOAT AS bottom_tick,
        utils.udf_hex_to_int(
            topic_3
        ) :: FLOAT AS top_tick,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: FLOAT AS liquidity_amount,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: FLOAT AS amount0,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: FLOAT AS amount1,
        protocol,
        version,
        TYPE,
        platform,
        _log_id,
        modified_timestamp
    FROM
        evt
    WHERE
        topic_0 = '0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c' --burn
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
        bottom_tick AS tick_lower,
        top_tick AS tick_upper,
        token0,
        token1,
        owner_address,
        sender_address AS sender,
        owner_address AS receiver,
        liquidity_amount AS liquidity_amount_unadj,
        amount0 AS amount0_unadj,
        amount1 AS amount1_unadj,
        protocol,
        version,
        TYPE,
        platform,
        _log_id,
        modified_timestamp
    FROM
        mint
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
        bottom_tick AS tick_lower,
        top_tick AS tick_upper,
        token0,
        token1,
        owner_address,
        owner_address AS sender,
        origin_from_address AS receiver,
        liquidity_amount AS liquidity_amount_unadj,
        amount0 AS amount0_unadj,
        amount1 AS amount1_unadj,
        protocol,
        version,
        TYPE,
        platform,
        _log_id,
        modified_timestamp
    FROM
        burn
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
    tick_lower,
    tick_upper,
    token0,
    token1,
    origin_from_address AS liquidity_provider,
    owner_address,
    sender,
    receiver,
    liquidity_amount_unadj,
    amount0_unadj,
    amount1_unadj,
    protocol,
    version,
    TYPE,
    platform,
    _log_id,
    modified_timestamp
FROM
    all_actions qualify(ROW_NUMBER() over (PARTITION BY _log_id
ORDER BY
    modified_timestamp DESC)) = 1
