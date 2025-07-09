{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_bridge','defi','bridge','curated']
) }}

WITH regular AS (

    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        contract_address,
        part,
        intent_id,
        initiator,
        -- might use origin from address instead , if it's 0x15a7ca97d1ed168fb34a4055cefa2e2f9bdb6c75
        receiver,
        input_asset,
        output_asset_raw,
        -- will be address 0 if dst > 1
        source_chain_id,
        amount_raw,
        destination_count,
        destination_0,
        _log_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver_bridge__everclear_intent_added') }}
    WHERE
        destination_count = 1

{% if is_incremental() %}
AND (
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp) - INTERVAL '{{ var("LOOKBACK", "12 hours") }}'
        FROM
            {{ this }}
    )
)
{% endif %}
),
edge AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        contract_address,
        part,
        intent_id,
        initiator,
        -- might use origin from address instead , if it's 0x15a7ca97d1ed168fb34a4055cefa2e2f9bdb6c75
        receiver,
        input_asset,
        output_asset_raw,
        -- will be address 0 if dst > 1
        source_chain_id,
        amount_raw,
        destination_count,
        destination_0,
        _log_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver_bridge__everclear_intent_added') }}
    WHERE
        destination_count > 1

{% if is_incremental() %}
AND (
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp) - INTERVAL '{{ var("LOOKBACK", "10 days") }}'
        FROM
            {{ this }}
    )
)
{% endif %}
),
intent_reads AS (
    SELECT
        intent_id,
        output_asset AS output_asset_reads,
        destination_chain_id AS destination_chain_id_reads
    FROM
        {{ ref('silver_bridge__everclear_reads') }}
    WHERE
        status = 'SETTLED_AND_COMPLETED'

{% if is_incremental() %}
AND intent_id NOT IN (
    SELECT
        intent_id
    FROM
        {{ this }}
)
{% endif %}
),
complete_edge AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        contract_address,
        part,
        intent_id,
        initiator,
        -- might use origin from address instead , if it's 0x15a7ca97d1ed168fb34a4055cefa2e2f9bdb6c75
        receiver,
        input_asset,
        output_asset_raw,
        output_asset_reads AS output_asset,
        source_chain_id,
        amount_raw,
        destination_count,
        destination_0,
        destination_chain_id_reads AS destination_chain_id,
        _log_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        edge
        INNER JOIN intent_reads USING (intent_id)
),
combined AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        contract_address,
        part,
        intent_id,
        initiator,
        -- might use origin from address instead , if it's 0x15a7ca97d1ed168fb34a4055cefa2e2f9bdb6c75
        receiver,
        input_asset,
        output_asset_raw AS output_asset,
        source_chain_id,
        amount_raw,
        destination_count,
        destination_0 AS destination_chain_id,
        _log_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        regular
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        contract_address,
        part,
        intent_id,
        initiator,
        -- might use origin from address instead , if it's 0x15a7ca97d1ed168fb34a4055cefa2e2f9bdb6c75
        receiver,
        input_asset,
        output_asset,
        source_chain_id,
        amount_raw,
        destination_count,
        destination_chain_id,
        _log_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        complete_edge
)
SELECT
    block_number,
    block_timestamp,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_hash,
    event_index,
    'IntentAdded' AS event_name,
    contract_address AS bridge_address,
    'everclear' AS platform,
    'v1' AS version,
    intent_id,
    origin_from_address AS sender,
    initiator,
    receiver,
    IFF(
        destination_chain_id = '1399811149',
        utils.udf_hex_to_base58(receiver),
        receiver
    ) AS destination_chain_receiver,
    input_asset,
    output_asset,
    input_asset AS token_address,
    amount_raw AS amount_unadj,
    destination_count,
    source_chain_id,
    destination_chain_id,
    chain AS destination_chain,
    _log_id,
    inserted_timestamp,
    modified_timestamp
FROM
    combined C
    LEFT JOIN {{ ref('silver_bridge__everclear_chain_seed') }}
    s
    ON C.destination_chain_id = s.chainid
    /* 
                                pull new intent added, make a call, join on the call results 
                                pull new intent, make a call but still in progress. dont want to pull the results 
                                next run, make a call, get results, want to pull in this results 
                                
                                */
