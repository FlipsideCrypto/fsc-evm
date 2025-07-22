{# Set variables #}
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
        protocol = 'everclear'
        AND version = 'v1'
),

events AS (

    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        l.contract_address,
        regexp_substr_all(SUBSTR(DATA, 3), '.{64}') AS part,
        topic_1 AS intent_id,
        '0x' || SUBSTR(
            part [2] :: STRING,
            25
        ) AS initiator,
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
        utils.udf_hex_to_int(
            part [7] :: STRING
        ) AS source_chain_id,
        utils.udf_hex_to_int(
            part [14] :: STRING
        ) :: INT AS destination_count,
        utils.udf_hex_to_int(
            part [15] :: STRING
        ) :: STRING AS destination_0,
        m.protocol,
        m.version,
        m.type,
        CONCAT(m.protocol, '-', m.version) AS platform,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }} 
        l
        INNER JOIN contract_mapping m
        ON l.contract_address = m.contract_address
    WHERE
        topic_0 = '0xefe68281645929e2db845c5b42e12f7c73485fb5f18737b7b29379da006fa5f7'
        AND block_timestamp :: DATE >= '2024-09-01'
        AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
{% endif %}
),
traces AS (
    SELECT
        tx_hash,
        regexp_substr_all(SUBSTR(input, 11), '.{64}') AS inputs,
        regexp_substr_all(SUBSTR(output, 3), '.{64}') AS outputs,
        utils.udf_hex_to_int(
            inputs [4] :: STRING
        ) AS amount_raw,
        '0x' || outputs [0] :: STRING AS intent_id
    FROM
        {{ ref('core__fact_traces') }}
        t
        INNER JOIN contract_mapping m
        ON t.to_address = m.contract_address
    WHERE
        block_timestamp :: DATE >= '2024-09-01'
        AND TYPE = 'CALL'
        AND LEFT(
            input,
            10
        ) IN (
            -- 3 versions of newIntent
            '0x4a943d21',
            -- address for senders
            '0x1b5c3e8b',
            -- bytes32 for senders
            '0xb4c20477' -- permit2
        )
        AND tx_succeeded
        AND trace_succeeded

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
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_hash,
    event_index,
    contract_address,
    intent_id,
    initiator,
    receiver,
    input_asset,
    output_asset_raw,
    source_chain_id,
    amount_raw,
    destination_count,
    destination_0,
    protocol,
    version,
    type,
    platform,
    _log_id,
    inserted_timestamp,
    modified_timestamp
FROM
    events
    INNER JOIN traces USING (
        tx_hash,
        intent_id
    )
