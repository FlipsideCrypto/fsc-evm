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
        INNER JOIN {{ref('silver_dex__pharaoh_v1_pools')}} p
        ON l.contract_address = p.pool_address
    WHERE
        topic_0 IN ('0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f', --mint
        '0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496' --burn
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
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS sender_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS amount0,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS amount1,
        protocol,
        version,
        type,
        platform,
        _log_id,
        modified_timestamp
    FROM
        evt
    WHERE
        topic_0 = '0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f' --mint
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
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS sender_address,
        CONCAT('0x', SUBSTR(topic_2, 27, 40)) AS to_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS amount0,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS amount1,
        protocol,
        version,
        type,
        platform,
        _log_id,
        modified_timestamp
    FROM
        evt
    WHERE
        topic_0 = '0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496' --burn
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
        pool_address AS receiver,
        amount0 AS amount0_unadj,
        amount1 AS amount1_unadj,
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
        token0,
        token1,
        sender_address AS sender,
        to_address AS receiver,
        amount0 AS amount0_unadj,
        amount1 AS amount1_unadj,
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
