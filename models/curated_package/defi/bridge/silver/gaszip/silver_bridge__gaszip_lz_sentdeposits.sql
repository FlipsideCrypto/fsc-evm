{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_bridge','defi','bridge','curated']
) }}

WITH contract_mapping AS (
    {{ curated_contract_mapping(
        vars.CURATED_DEFI_BRIDGE_CONTRACT_MAPPING
    ) }}
    WHERE
        protocol = 'gaszip_lz'
),
senddeposits AS (
    -- gaszip lz v2 event (only 1 per tx)

    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        l.contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS to_address,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [2] :: STRING)) AS VALUE,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [3] :: STRING)) AS fee,
        CONCAT('0x', SUBSTR(segmented_data [4] :: STRING, 25, 40)) AS from_address,
        m.protocol,
        m.version,
        CONCAT(
            m.protocol,
            '-',
            m.version
        ) AS platform,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        INNER JOIN contract_mapping m
        ON l.contract_address = m.contract_address
    WHERE
        topic_0 = '0xa22a487af6300dc77db439586e8ce7028fd7f1d734efd33b287bc1e2af4cd162' -- senddeposits
        AND tx_succeeded
        AND m.type = 'send_deposits'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
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
        l
        INNER JOIN contract_mapping m
        ON l.contract_address = m.contract_address
    WHERE
        topic_0 = '0x1ab700d4ced0c005b164c0f789fd09fcbb0156d4c2041b8a3bfbcd961cd1567f' -- packetsent
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                senddeposits
        )
        AND tx_succeeded
        AND m.type = 'packet_sent'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
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
        from_address IN (
            SELECT
                contract_address
            FROM
                contract_mapping
            WHERE
                type = 'packet_sent'
        )
        AND to_address IN (
            SELECT
                contract_address
            FROM
                contract_mapping
            WHERE
                type = 'send_uln'
        )
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                senddeposits
        )

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
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
    protocol,
    version,
    platform,
    CONCAT(
        s.tx_hash :: STRING,
        '-',
        p.event_index :: STRING
    ) AS _log_id,
    modified_timestamp
FROM
    senddeposits s
    INNER JOIN packetsent p
    ON s.tx_hash = p.tx_hash
    LEFT JOIN nativetransfers t
    ON p.tx_hash = t.tx_hash
    AND event_rank = transfer_rank
    LEFT JOIN {{ ref('silver_bridge__layerzero_v2_bridge_seed') }}
    ON dstEid = eid
