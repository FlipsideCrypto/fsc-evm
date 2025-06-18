{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    tags = ['silver_bridge','defi','bridge','curated']
) }}

WITH bus_driven AS (

    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        event_name,
        stargate_adapter_address AS bridge_address,
        'stargate' AS platform,
        'v2' AS version,
        guid,
        ticket_id,
        from_address AS sender,
        dst_receiver_address AS receiver,
        dst_receiver_address AS destination_chain_receiver,
        dst_chain_id,
        dst_chain_id :: STRING AS destination_chain_id,
        dst_chain AS destination_chain,
        asset_address AS token_address,
        NULL AS token_symbol,
        amount_sent AS amount_unadj,
        src_chain_id,
        src_chain,
        asset_id,
        asset_name,
        payload,
        TYPE,
        nonce,
        sender_contract_address,
        receiver_contract_address,
        message_type,
        CONCAT(
            tx_hash,
            '-',
            event_index,
            '-',
            ticket_id
        ) AS _log_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver_bridge__stargate_v2_busdriven') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp) - INTERVAL '{{ var("LOOKBACK", "12 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
oft_sent AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        'OFTSent' AS event_name,
        contract_address AS bridge_address,
        'stargate' AS platform,
        'v2' AS version,
        guid,
        NULL AS ticket_id,
        from_address AS sender,
        to_address AS receiver,
        to_address AS destination_chain_receiver,
        dst_chain_id,
        dst_chain_id :: STRING AS destination_chain_id,
        dst_chain AS destination_chain,
        token_address,
        NULL AS token_symbol,
        amount_sent AS amount_unadj,
        src_chain_id,
        src_chain,
        asset_id,
        asset_name,
        payload,
        TYPE,
        nonce,
        sender_contract_address,
        receiver_contract_address,
        message_type,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver_bridge__stargate_v2_oft') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp) - INTERVAL '{{ var("LOOKBACK", "12 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
combined AS (
    SELECT
        *
    FROM
        bus_driven
    UNION ALL
    SELECT
        *
    FROM
        oft_sent
),
FINAL AS (
    SELECT
        *
    FROM
        combined

{% if is_incremental() and 'stargate_asset' in var('HEAL_MODELS') %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_hash,
    event_index,
    event_name,
    bridge_address,
    platform,
    version,
    guid,
    ticket_id,
    sender,
    receiver,
    destination_chain_receiver,
    dst_chain_id,
    destination_chain_id,
    LOWER(
        b.chain
    ) AS destination_chain,
    A.address AS token_address,
    A.asset AS token_symbol,
    amount_unadj,
    src_chain_id,
    src_chain,
    A.id AS asset_id,
    A.asset AS asset_name,
    payload,
    TYPE,
    nonce,
    sender_contract_address,
    receiver_contract_address,
    message_type,
    _log_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ this }}
    t
    LEFT JOIN {{ ref('silver_bridge__stargate_asset_id_seed') }} A
    ON t.bridge_address = A.oftaddress
    AND A.chain = 'Base'
    LEFT JOIN {{ ref('silver_bridge__layerzero_bridge_seed') }}
    b
    ON t.destination_chain_id = b.eid
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_hash,
    event_index,
    event_name,
    bridge_address,
    platform,
    version,
    guid,
    ticket_id,
    sender,
    receiver,
    destination_chain_receiver,
    dst_chain_id,
    destination_chain_id,
    destination_chain,
    token_address,
    token_symbol,
    amount_unadj,
    src_chain_id,
    src_chain,
    asset_id,
    asset_name,
    payload,
    TYPE,
    nonce,
    sender_contract_address,
    receiver_contract_address,
    message_type,
    _log_id,
    inserted_timestamp,
    modified_timestamp
FROM
    FINAL qualify ROW_NUMBER() over (
        PARTITION BY _log_id
        ORDER BY
            modified_timestamp DESC,
            destination_chain DESC nulls last
    ) = 1
