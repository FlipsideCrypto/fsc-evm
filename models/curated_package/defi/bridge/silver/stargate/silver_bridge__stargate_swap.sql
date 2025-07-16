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
        protocol = 'stargate'
        AND version = 'v1'
        AND type = 'bridge'
),
base_evt AS (
    SELECT
        d.block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        d.contract_address,
        m.contract_address AS bridge_address,
        event_index,
        topics [0] :: STRING AS topic_0,
        event_name,
        TRY_TO_NUMBER(
            decoded_log :"amountSD" :: STRING
        ) AS amountSD,
        TRY_TO_NUMBER(
            decoded_log :"chainId" :: STRING
        ) AS chainId,
        CASE
            WHEN chainId < 100 THEN chainId + 100
            ELSE chainId
        END AS destination_chain_id,
        TRY_TO_NUMBER(
            decoded_log :"dstPoolId" :: STRING
        ) AS dstPoolId,
        TRY_TO_NUMBER(
            decoded_log :"eqFee" :: STRING
        ) AS eqFee,
        TRY_TO_NUMBER(
            decoded_log :"eqReward" :: STRING
        ) AS eqReward,
        TRY_TO_NUMBER(
            decoded_log :"amountSD" :: STRING
        ) AS lpFee,
        TRY_TO_NUMBER(
            decoded_log :"amountSD" :: STRING
        ) AS protocolFee,
        decoded_log :"from" :: STRING AS from_address,
        decoded_log,
        token_address,
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
        d
        INNER JOIN {{ ref('silver_bridge__stargate_createpool') }} p
        ON d.contract_address = p.pool_address
        CROSS JOIN contract_mapping m
    WHERE
        topics [0] :: STRING = '0x34660fc8af304464529f48a778e03d03e4d34bcd5f9b6f0cfbf3cd238c642f7f'
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
    bridge_address,
    from_address AS sender,
    from_address AS receiver,
    receiver AS destination_chain_receiver,
    amountSD AS amount_unadj,
    destination_chain_id,
    LOWER(chain_name) AS destination_chain,
    dstPoolId AS destination_pool_id,
    eqFee AS fee,
    eqReward AS reward,
    lpFee AS lp_fee,
    protocolFee AS protocol_fee,
    token_address,
    protocol,
    version,
    type,
    platform,
    _log_id,
    modified_timestamp
FROM
    base_evt b
    LEFT JOIN {{ ref('silver_bridge__stargate_chain_id_seed') }}
    s
    ON b.destination_chain_id :: STRING = s.chain_id :: STRING
