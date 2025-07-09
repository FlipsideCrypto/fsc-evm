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
        protocol = 'celer_cbridge'
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
        TRY_TO_NUMBER(
            decoded_log :"amount" :: STRING
        ) AS amount,
        TRY_TO_NUMBER(
            decoded_log :"dstChainId" :: STRING
        ) AS dstChainId,
        TRY_TO_NUMBER(
            decoded_log :"maxSlippage" :: STRING
        ) AS maxSlippage,
        TRY_TO_NUMBER(
            decoded_log :"nonce" :: STRING
        ) AS nonce,
        decoded_log :"receiver" :: STRING AS receiver,
        decoded_log :"sender" :: STRING AS sender,
        decoded_log :"token" :: STRING AS token,
        decoded_log :"transferId" :: STRING AS transferId,
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
        topics [0] :: STRING = '0x89d8051e597ab4178a863a5190407b98abfeff406aa8db90c59af76612e58f01'
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
    sender,
    receiver,
    receiver AS destination_chain_receiver,
    amount,
    dstChainId AS destination_chain_id,
    maxSlippage AS max_slippage,
    nonce,
    token AS token_address,
    transferId AS transfer_id,
    protocol,
    version,
    platform,
    _log_id,
    modified_timestamp
FROM
    base_evt
