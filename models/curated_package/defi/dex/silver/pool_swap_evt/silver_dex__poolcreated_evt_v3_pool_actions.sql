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
        token0_address AS token_0,
        token1_address AS token_1,
        fee,
        fee_percent,
        tick_spacing,
        init_tick,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        data,
        regexp_substr_all(SUBSTR(data, 3, len(data)), '.{64}') AS segmented_data,
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
        INNER JOIN {{ref('silver_dex__poolcreated_evt_v3_pools')}} p
        ON l.contract_address = p.pool_address
    WHERE
        topic_0 IN ('0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde', --mint
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
        token_0,
        token_1,
        fee,
        fee_percent,
        tick_spacing,
        init_tick,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS owner_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                topic_2
            )
        ) AS tick_lower,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                topic_3
            )
        ) AS tick_upper,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS sender_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS amount,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            )
        ) AS amount_0,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [3] :: STRING
            )
        ) AS amount_1,
        protocol,
        version,
        type,
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
        token_0,
        token_1,
        fee,
        fee_percent,
        tick_spacing,
        init_tick,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS owner_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                topic_2
            )
        ) AS tick_lower,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                topic_3
            )
        ) AS tick_upper,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS amount,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS amount_0,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            )
        ) AS amount_1,
        protocol,
        version,
        type,
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
        fee,
        fee_percent,
        tick_spacing,
        init_tick,
        tick_lower,
        tick_upper,
        token_0,
        token_1,
        owner_address,
        sender_address AS sender,
        owner_address AS receiver,
        amount AS amount_unadj,
        amount_0 AS amount_0_unadj,
        amount_1 AS amount_1_unadj,
        protocol,
        version,
        type,
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
        fee,
        fee_percent,
        tick_spacing,
        init_tick,
        tick_lower,
        tick_upper,
        token_0,
        token_1,
        owner_address,
        owner_address AS sender,
        origin_from_address AS receiver,
        amount AS amount_unadj,
        amount_0 AS amount_0_unadj,
        amount_1 AS amount_1_unadj,
        protocol,
        version,
        type,
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
    fee,
    fee_percent,
    tick_spacing,
    init_tick,
    tick_lower,
    tick_upper,
    token_0,
    token_1,
    owner_address,
    sender,
    receiver,
    amount_unadj,
    amount_0_unadj,
    amount_1_unadj,
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
