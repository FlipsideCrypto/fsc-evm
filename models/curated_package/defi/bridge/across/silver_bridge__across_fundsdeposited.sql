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
        protocol = 'across'
        AND version = 'v1'
),
base_evt AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        topics [0] :: STRING AS topic_0,
        event_name,
        TRY_TO_NUMBER(
            decoded_log :"amount" :: STRING
        ) AS amount,
        TRY_TO_NUMBER(
            decoded_log :"depositId" :: STRING
        ) AS depositId,
        decoded_log :"depositor" :: STRING AS depositor,
        TRY_TO_NUMBER(
            decoded_log :"destinationChainId" :: STRING
        ) AS destinationChainId,
        decoded_log :"message" :: STRING AS message,
        TRY_TO_NUMBER(
            decoded_log :"originChainId" :: STRING
        ) AS originChainId,
        decoded_log :"originToken" :: STRING AS originToken,
        TRY_TO_TIMESTAMP(
            decoded_log :"quoteTimestamp" :: STRING
        ) AS quoteTimestamp,
        decoded_log :"recipient" :: STRING AS recipient,
        TRY_TO_NUMBER(
            decoded_log :"relayerFeePct" :: STRING
        ) AS relayerFeePct,
        decoded_log,
        event_removed,
        tx_succeeded,
        m.protocol,
        m.version,
        CONCAT(m.protocol, '-', m.version) AS platform,
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
        topics [0] :: STRING = '0xafc4df6845a4ab948b492800d3d8a25d538a102a2bc07cd01f1cfa097fddcff6'
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
)
SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    topic_0,
    event_name,
    event_removed,
    tx_succeeded,
    contract_address AS bridge_address,
    depositor AS sender,
    recipient AS receiver,
    recipient AS destination_chain_receiver,
    destinationChainId AS destination_chain_id,
    amount,
    depositId AS deposit_id,
    message,
    originChainId AS origin_chain_id,
    originToken AS token_address,
    quoteTimestamp AS quote_timestamp,
    relayerFeePct AS relayer_fee_pct,
    protocol,
    version,
    platform,
    _log_id,
    modified_timestamp
FROM
    base_evt
