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
        protocol = 'synapse'
        AND version = 'v1'
        AND type = 'token_bridge_swap'
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
            decoded_log :"chainId" :: STRING
        ) AS chainId,
        TRY_TO_TIMESTAMP(
            decoded_log :"deadline" :: STRING
        ) AS deadline,
        TRY_TO_NUMBER(
            decoded_log :"minDy" :: STRING
        ) AS minDy,
        decoded_log :"to" :: STRING AS to_address,
        decoded_log :"token" :: STRING AS token,
        TRY_TO_NUMBER(
            decoded_log :"tokenIndexFrom" :: STRING
        ) AS tokenIndexFrom,
        TRY_TO_NUMBER(
            decoded_log :"tokenIndexTo" :: STRING
        ) AS tokenIndexTo,
        decoded_log,
        event_removed,
        tx_succeeded,
        m.protocol,
        m.version,
        m.type,
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
            '0x91f25e9be0134ec851830e0e76dc71e06f9dade75a9b84e9524071dbbc319425',
            --redeemandswap
            '0x79c15604b92ef54d3f61f0c40caab8857927ca3d5092367163b4562c1699eb5f' --depositandswap
        )
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
    origin_from_address AS sender,
    to_address AS receiver,
    receiver AS destination_chain_receiver,
    amount,
    chainId AS destination_chain_id,
    token AS token_address,
    deadline,
    minDy AS min_dy,
    tokenIndexFrom AS token_index_from,
    tokenIndexTo AS token_index_to,
    protocol,
    version,
    type,
    platform,
    _log_id,
    modified_timestamp
FROM
    base_evt
