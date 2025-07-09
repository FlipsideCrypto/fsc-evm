{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_bridge','defi','bridge','curated']
) }}

WITH raw AS (

    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3), '.{64}') AS part,
        topic_1 AS intent_id,
        '0x' || SUBSTR(
            part [2] :: STRING,
            25
        ) AS initiator,
        -- might use origin from address instead , if it's 0x15a7ca97d1ed168fb34a4055cefa2e2f9bdb6c75
        '0x' || SUBSTR(
            part [3] :: STRING,
            25
        ) AS receiver,
        '0x' || SUBSTR(
            part [4] :: STRING,
            25
        ) AS input_asset,
        '0x' || SUBSTR(
            part [5] :: STRING,
            25
        ) AS output_asset_raw,
        -- will be address 0 if dst > 1
        utils.udf_hex_to_int(
            part [7] :: STRING
        ) AS source_chain_id,
        utils.udf_hex_to_int(
            part [11] :: STRING
        ) AS amount_raw,
        utils.udf_hex_to_int(
            part [14] :: STRING
        ) :: INT AS destination_count,
        utils.udf_hex_to_int(
            part [15] :: STRING
        ) :: STRING AS destination_0,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address = LOWER('0xa05A3380889115bf313f1Db9d5f335157Be4D816')
        AND topic_0 = '0xefe68281645929e2db845c5b42e12f7c73485fb5f18737b7b29379da006fa5f7'
        AND block_timestamp :: DATE >= '2024-09-15'
        AND tx_succeeded

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
    raw
