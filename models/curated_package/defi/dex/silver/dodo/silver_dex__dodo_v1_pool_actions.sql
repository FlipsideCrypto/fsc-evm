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
        base_token AS token0,
        quote_token AS token1,
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
        INNER JOIN {{ref('silver_dex__dodo_v1_pools')}} p
        ON l.contract_address = p.pool_address
    WHERE
        topic_0 IN ('0x18081cde2fa64894914e1080b98cca17bb6d1acf633e57f6e26ebdb945ad830b', --deposit
        '0xe89c586bd81ee35a18f7eac22a732b56e589a2821497cce12a0208828540a36d' --withdraw
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
        'Deposit' AS event_name,
        pool_address,
        token0,
        token1,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS payer_address,
        CONCAT('0x', SUBSTR(topic_2, 27, 40)) AS receiver_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS is_base_token_raw,
        CASE 
            WHEN is_base_token_raw = 1 THEN TRUE 
            ELSE FALSE
        END AS is_base_token,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS amount,
        CASE
            WHEN is_base_token THEN amount
            ELSE 0
        END AS amount0,
        CASE
            WHEN NOT is_base_token THEN amount
            ELSE 0
        END AS amount1,
        --deposits occur for each token separately
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            )
        ) AS lp_token_amount,
        CASE
            WHEN is_base_token THEN lp_token_amount
            ELSE 0
        END AS lp_token_amount0,
        CASE
            WHEN NOT is_base_token THEN lp_token_amount
            ELSE 0
        END AS lp_token_amount1,
        protocol,
        version,
        type,
        platform,
        _log_id,
        modified_timestamp
    FROM
        evt
    WHERE
        topic_0 = '0x18081cde2fa64894914e1080b98cca17bb6d1acf633e57f6e26ebdb945ad830b' --deposit
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
        'Withdraw' AS event_name,
        pool_address,
        token0,
        token1,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS payer_address,
        CONCAT('0x', SUBSTR(topic_2, 27, 40)) AS receiver_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS is_base_token_raw,
        CASE 
            WHEN is_base_token_raw = 1 THEN TRUE 
            ELSE FALSE
        END AS is_base_token,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS amount,
        CASE
            WHEN is_base_token THEN amount
            ELSE 0
        END AS amount0,
        CASE
            WHEN NOT is_base_token THEN amount
            ELSE 0
        END AS amount1,
        --withdraws occur for each token separately
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            )
        ) AS lp_token_amount,
        CASE
            WHEN is_base_token THEN lp_token_amount
            ELSE 0
        END AS lp_token_amount0,
        CASE
            WHEN NOT is_base_token THEN lp_token_amount
            ELSE 0
        END AS lp_token_amount1,
        protocol,
        version,
        type,
        platform,
        _log_id,
        modified_timestamp
    FROM
        evt
    WHERE
        topic_0 = '0xe89c586bd81ee35a18f7eac22a732b56e589a2821497cce12a0208828540a36d' --withdraw
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
        payer_address AS sender,
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
        payer_address AS sender,
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
