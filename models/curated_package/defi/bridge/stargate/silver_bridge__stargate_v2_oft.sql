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

WITH layerzero AS (

    SELECT
        tx_hash,
        payload,
        TYPE,
        nonce,
        src_chain_id,
        src_chain,
        sender_contract_address,
        dst_chain_id,
        dst_chain,
        receiver_contract_address,
        guid,
        message_type,
        SUBSTR(
            payload,
            229,
            4
        ) AS message_type_2,
        '0x' || SUBSTR(SUBSTR(payload, 233, 64), 25) AS to_address
    FROM
        {{ ref('silver_bridge__layerzero_v2_packet') }}
    WHERE
        sender_contract_address = '{{ vars.CURATED_BRIDGE_STARGATE_TOKEN_MESSAGING_CONTRACT }}'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ var("LOOKBACK", "12 hours") }}'
    FROM
        {{ this }}
)
{% endif %}
),
oft_raw AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        contract_address AS stargate_oft_address,
        DATA,
        regexp_substr_all(SUBSTR(DATA, 3), '.{64}') AS part,
        SUBSTR(
            topic_1,
            3
        ) AS guid,
        '0x' || SUBSTR(
            topic_2,
            27
        ) AS from_address,
        utils.udf_hex_to_int(
            part [0] :: STRING
        ) :: INT AS dst_chain_id_oft,
        utils.udf_hex_to_int(
            part [1] :: STRING
        ) :: INT AS amount_sent,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        block_timestamp :: DATE >= '2024-01-01'
        AND topic_0 = '0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a' --OFTSent

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ var("LOOKBACK", "12 hours") }}'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    guid,
    event_index,
    'OFTSent' AS event_name,
    stargate_oft_address,
    stargate_oft_address AS contract_address,
    A.address AS token_address,
    A.id AS asset_id,
    A.asset AS asset_name,
    from_address,
    to_address,
    src_chain_id,
    src_chain,
    dst_chain_id,
    dst_chain,
    dst_chain_id_oft,
    amount_sent,
    payload,
    TYPE,
    nonce,
    sender_contract_address,
    receiver_contract_address,
    message_type,
    message_type_2,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    inserted_timestamp,
    modified_timestamp
FROM
    oft_raw o
    INNER JOIN layerzero l USING (
        tx_hash,
        guid
    )
    LEFT JOIN {{ ref('silver_bridge__stargate_v2_asset_seed') }} A
    ON o.stargate_oft_address = A.oftaddress
    AND A.chain = '{{ vars.GLOBAL_PROJECT_NAME }}'
