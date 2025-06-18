{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    tags = ['silver_bridge','defi','bridge','curated']
) }}

WITH bus_driven_raw AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        contract_address,
        decoded_log :dstEid :: INT AS dst_id,
        SUBSTR(
            decoded_log :guid :: STRING,
            3
        ) AS guid,
        decoded_log :numPassengers :: INT AS num_passengers,
        decoded_log :startTicketId :: INT AS start_ticket_id,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        contract_address = '0x5634c4a5fed09819e3c46d86a965dd9447d86e47'
        AND event_name = 'BusDriven'
        AND block_timestamp :: DATE >= '2024-01-01'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
bus_driven_array AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        contract_address,
        dst_id,
        guid,
        num_passengers,
        start_ticket_id,
        VALUE :: INT AS ticket_id,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        inserted_timestamp,
        modified_timestamp
    FROM
        bus_driven_raw,
        LATERAL FLATTEN (
            input =>(
                array_generate_range(
                    start_ticket_id,
                    start_ticket_id + num_passengers,
                    1
                )
            )
        )
),
bus_driven AS (
    SELECT
        r.block_number,
        r.block_timestamp,
        r.tx_hash,
        r.event_index,
        r.event_name,
        r.guid,
        contract_address,
        stargate_adapter_address,
        r.dst_id,
        start_ticket_id,
        num_passengers,
        b.ticket_id,
        asset_id,
        asset_name,
        asset_address,
        from_address,
        dst_receiver_address,
        b.amount_sent,
        r.origin_from_address,
        r.origin_to_address,
        r.origin_function_signature,
        r.inserted_timestamp,
        r.modified_timestamp
    FROM
        bus_driven_array r
        INNER JOIN {{ ref('silver_bridge__stargate_v2_busrode') }}
        b USING (
            dst_id,
            ticket_id
        )

{% if is_incremental() %}
WHERE
    b.modified_timestamp >= (
        SELECT
            MAX(modified_timestamp) - INTERVAL '{{ var("LOOKBACK", "12 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
layerzero AS (
    SELECT
        tx_hash,
        guid,
        payload,
        TYPE,
        nonce,
        src_chain_id,
        src_chain,
        sender_contract_address,
        dst_chain_id,
        dst_chain,
        receiver_contract_address,
        message_type
    FROM
        {{ ref('silver_bridge__layerzero_v2_packet') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    b.block_number,
    b.block_timestamp,
    b.tx_hash,
    guid,
    b.event_index,
    b.event_name,
    b.contract_address,
    stargate_adapter_address,
    dst_id,
    start_ticket_id,
    num_passengers,
    ticket_id,
    asset_id,
    asset_name,
    asset_address,
    from_address,
    dst_receiver_address,
    amount_sent,
    payload,
    TYPE,
    nonce,
    src_chain_id,
    src_chain,
    sender_contract_address,
    dst_chain_id,
    dst_chain,
    receiver_contract_address,
    message_type,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    inserted_timestamp,
    modified_timestamp
FROM
    bus_driven b
    INNER JOIN layerzero l USING (
        tx_hash,
        guid
    )
