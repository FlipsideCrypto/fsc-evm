{# Set variables #}
{% set vars = return_vars() %}
{# Log configuration details #}
{{ log_model_details() }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    tags = ['silver_bridge','defi','bridge','curated']
) }}

WITH raw AS (

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
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        block_timestamp :: DATE >= '2024-01-01'
        AND event_name = 'PacketSent'
        AND contract_address = '{{ vars.CURATED_BRIDGE_LAYERZERO_ENDPOINT_V2_CONTRACT }}'

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp) - INTERVAL '{{ var("LOOKBACK", "12 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    block_timestamp,
    tx_hash,
    event_index,
    payload,
    TYPE,
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
    inserted_timestamp,
    modified_timestamp
FROM
    raw
    LEFT JOIN {{ ref('silver_bridge__layerzero_v2_bridge_seed') }}
    c1
    ON src_chain_id = c1.eid
    LEFT JOIN {{ ref('silver_bridge__layerzero_v2_bridge_seed') }}
    c2
    ON dst_chain_id = c2.eid
