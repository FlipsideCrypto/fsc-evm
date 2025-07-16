{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    tags = ['silver_bridge','defi','bridge','curated']
) }}

WITH contract_mapping AS (
    {{ curated_contract_mapping(
        vars.CURATED_DEFI_BRIDGE_CONTRACT_MAPPING
    ) }}
    WHERE
        protocol = 'layerzero'
        AND version = 'v2'
),
raw AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        SUBSTR(
            decoded_log :encodedPayload :: STRING,
            3
        ) AS payload,
        SUBSTR(
            payload,
            1,
            2
        ) AS TYPE,
        SUBSTR(
            payload,
            3,
            16
        ) AS nonce,
        utils.udf_hex_to_int(SUBSTR(payload, 19, 8)) AS src_chain_id,
        '0x' || SUBSTR(SUBSTR(payload, 27, 64), 25) AS sender_contract_address,
        utils.udf_hex_to_int(SUBSTR(payload, 91, 8)) AS dst_chain_id,
        '0x' || SUBSTR(SUBSTR(payload, 99, 64), 25) AS receiver_contract_address,
        SUBSTR(
            payload,
            163,
            64
        ) AS guid,
        SUBSTR(
            payload,
            227,
            2
        ) AS message_type,
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
        event_name = 'PacketSent'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    payload,
    TYPE AS tx_type,
    nonce,
    src_chain_id,
    LOWER(
        c1.chain
    ) AS src_chain,
    sender_contract_address,
    dst_chain_id,
    LOWER(
        c2.chain
    ) AS dst_chain,
    receiver_contract_address,
    guid,
    message_type,
    protocol,
    version,
    type,
    platform,
    _log_id,
    modified_timestamp
FROM
    raw
    LEFT JOIN {{ ref('silver_bridge__layerzero_v2_bridge_seed') }}
    c1
    ON src_chain_id = c1.eid
    LEFT JOIN {{ ref('silver_bridge__layerzero_v2_bridge_seed') }}
    c2
    ON dst_chain_id = c2.eid
