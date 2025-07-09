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
        protocol = 'eywa'
),
base_evt AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        l.contract_address,
        event_index,
        topics [0] :: STRING AS topic_0,
        event_name,
        decoded_log,
        event_removed,
        tx_succeeded,
        m.protocol,
        m.version,
        CONCAT(
            m.protocol,
            '-',
            m.version
        ) AS platform,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }} 
        l
        INNER JOIN contract_mapping m
        ON l.contract_address = m.contract_address
    WHERE
        topics [0] :: STRING IN (
            '0x5566d73d091d945ab32ea023cd1930c0d43aa43bef9aee4cb029775cfc94bdae',
            --RequestSent
            '0xb5f411fa3c897c9b0b6cd61852278a67e73d885610724a5610a8580d3e94cfdb'
        ) --locked
        AND tx_succeeded

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
requestsent AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        NAME,
        event_index,
        topic_0,
        event_name,
        decoded_log :"chainIdTo" :: STRING AS chainIdTo,
        decoded_log :"data" :: STRING AS data_requestsent,
        decoded_log :"requestId" :: STRING AS requestId,
        decoded_log :"to" :: STRING AS to_address,
        decoded_log,
        event_removed,
        tx_succeeded,
        protocol,
        version,
        platform,
        _log_id,
        modified_timestamp
    FROM
        base_evt
    WHERE
        topic_0 = '0x5566d73d091d945ab32ea023cd1930c0d43aa43bef9aee4cb029775cfc94bdae' --RequestSent
),
locked AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        NAME,
        event_index,
        topic_0,
        event_name,
        TRY_TO_NUMBER(
            decoded_log :"amount" :: STRING
        ) AS amount,
        decoded_log :"from" :: STRING AS from_address,
        decoded_log :"to" :: STRING AS to_address,
        decoded_log :"token" :: STRING AS token,
        decoded_log,
        event_removed,
        tx_succeeded,
        protocol,
        version,
        platform,
        _log_id,
        modified_timestamp
    FROM
        base_evt
    WHERE
        topic_0 = '0xb5f411fa3c897c9b0b6cd61852278a67e73d885610724a5610a8580d3e94cfdb' --Locked
)
SELECT
    r.block_number,
    r.block_timestamp,
    r.origin_function_signature,
    r.origin_from_address,
    r.origin_to_address,
    r.tx_hash,
    r.event_index,
    r.topic_0,
    r.event_name,
    r.event_removed,
    r.tx_status,
    r.contract_address AS bridge_address,
    r.name AS platform,
    l.from_address AS sender,
    sender AS receiver,
    receiver AS destination_chain_receiver,
    l.amount,
    r.chainIdTo AS destination_chain_id,
    l.token AS token_address,
    r.protocol,
    r.version,
    r.platform,
    r._log_id,
    r.modified_timestamp
FROM
    requestsent r
    LEFT JOIN locked l USING(
        block_number,
        tx_hash
    )
WHERE token_address IS NOT NULL
