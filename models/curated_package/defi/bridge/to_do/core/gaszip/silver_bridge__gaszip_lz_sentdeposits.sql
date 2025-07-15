{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_bridge','defi','bridge','curated']
) }}

WITH senddeposits AS (
    -- gaszip lz v2 event (only 1 per tx)

    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS to_address,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [2] :: STRING)) AS VALUE,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [3] :: STRING)) AS fee,
        CONCAT('0x', SUBSTR(segmented_data [4] :: STRING, 25, 40)) AS from_address,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address = '0x26da582889f59eaae9da1f063be0140cd93e6a4f' -- gaszip l0 v2
        AND topic_0 = '0xa22a487af6300dc77db439586e8ce7028fd7f1d734efd33b287bc1e2af4cd162' -- senddeposits
        AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
packetsent AS (
    -- pulls lz packetsent events from gaszip txs only (1 packet per chain, may have >1 per tx)
    SELECT
        tx_hash,
        event_index,
        DATA,
        CONCAT('0x', SUBSTR(DATA, 155, 40)) AS send_lib,
        utils.udf_hex_to_int(SUBSTR(DATA, 261, 16)) AS nonce,
        utils.udf_hex_to_int(SUBSTR(DATA, 277, 8)) AS srcEid,
        CONCAT('0x', SUBSTR(DATA, 258 + 18 + 8 + 25, 40)) AS src_app_address,
        utils.udf_hex_to_int(SUBSTR(DATA, 258 + 18 + 8 + 64 + 1, 8)) AS dstEid,
        CONCAT('0x', SUBSTR(DATA, 258 + 18 + 8 + 64 + 8 + 25, 40)) AS dst_app_address,
        TRY_TO_NUMBER(utils.udf_hex_to_int(SUBSTR(DATA, 630 + 1, 32))) AS native_amount,
        CONCAT('0x', SUBSTR(DATA, 630 + 1 + 32 + 24, 40)) AS receiver,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) event_rank
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address = '0x1a44076050125825900e736c501f859c50fe728c' -- l0 endpoint v2
        AND topic_0 = '0x1ab700d4ced0c005b164c0f789fd09fcbb0156d4c2041b8a3bfbcd961cd1567f' -- packetsent
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                senddeposits
        )
        AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
nativetransfers AS (
    -- pulls native transfers in gaszip lz v2 bridging
    SELECT
        tx_hash,
        TRY_TO_NUMBER(amount_precise_raw) AS amount_precise_raw,
        '0x40375c92d9faf44d2f9db9bd9ba41a3317a2404f' AS token_address,
        -- wrapped native
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) transfer_rank
    FROM
        {{ ref('core__ez_native_transfers') }}
    WHERE
        from_address = '0x1a44076050125825900e736c501f859c50fe728c' -- l0 endpoint v2
        AND to_address = '0x0bcac336466ef7f1e0b5c184aab2867c108331af' -- SendUln302
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                senddeposits
        )

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    s.tx_hash,
    p.event_index,
    -- joins on packetsent event index instead of senddeposits for uniqueness
    'SendDeposit' AS event_name,
    'gaszip-lz-v2' AS platform,
    'v2' AS version,
    contract_address AS bridge_address,
    contract_address,
    from_address AS sender,
    receiver,
    receiver AS destination_chain_receiver,
    nonce,
    dstEid AS destination_chain_id,
    chain AS destination_chain,
    amount_precise_raw AS amount_unadj,
    token_address,
    CONCAT(
        s.tx_hash :: STRING,
        '-',
        p.event_index :: STRING
    ) AS _log_id,
    _inserted_timestamp
FROM
    senddeposits s
    INNER JOIN packetsent p
    ON s.tx_hash = p.tx_hash
    LEFT JOIN nativetransfers t
    ON p.tx_hash = t.tx_hash
    AND event_rank = transfer_rank
    LEFT JOIN {{ ref('silver_bridge__layerzero_v2_bridge_seed') }}
    ON dstEid = eid
