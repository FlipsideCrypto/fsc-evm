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
        protocol = 'circle_cctp'
        AND version = 'v1'
),
base_evt AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        l.contract_address,
        event_index,
        TRY_TO_NUMBER(utils.udf_hex_to_int(topic_1 :: STRING)) AS nonce,
        CONCAT('0x', SUBSTR(topic_2 :: STRING, 27, 40)) AS burn_token,
        CONCAT('0x', SUBSTR(topic_3 :: STRING, 27, 40)) AS depositor,
        regexp_SUBSTR_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [0] :: STRING)) AS burn_amount,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [2] :: STRING)) AS destination_domain,
        CASE
            WHEN destination_domain IN (
                0,
                1,
                2,
                3,
                6,
                7,
                10
            ) THEN CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) -- evm
            WHEN destination_domain = 5 THEN utils.udf_hex_to_base58(CONCAT('0x', segmented_data [1] :: STRING)) -- solana
            ELSE CONCAT(
                '0x',
                segmented_data [1] :: STRING
            ) -- other non-evm chains
        END AS mint_recipient,
        CASE
            WHEN destination_domain IN (
                0,
                1,
                2,
                3,
                6,
                7,
                10
            ) THEN CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40)) -- evm
            WHEN destination_domain = 5 THEN utils.udf_hex_to_base58(CONCAT('0x', segmented_data [3] :: STRING)) -- solana
            ELSE CONCAT(
                '0x',
                segmented_data [3] :: STRING
            ) -- other non-evm chains
        END AS destinationTokenMessenger,
        CASE
            WHEN destination_domain IN (
                0,
                1,
                2,
                3,
                6,
                7,
                10
            ) THEN CONCAT('0x', SUBSTR(segmented_data [4] :: STRING, 25, 40)) -- evm
            WHEN destination_domain = 5 THEN utils.udf_hex_to_base58(CONCAT('0x', segmented_data [4] :: STRING)) -- solana
            ELSE CONCAT(
                '0x',
                segmented_data [4] :: STRING
            ) -- other non-evm chains
        END AS destination_caller,
        m.protocol,
        m.version,
        CONCAT(
            m.protocol,
            '-',
            m.version
        ) AS platform,
        'DepositForBurn' AS event_name,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        INNER JOIN contract_mapping m
        ON l.contract_address = m.contract_address
    WHERE
        topic_0 = '0x2fa9ca894982930190727e75500a97d8dc500233a5065e0f3126c48fbe0343c0'
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
    tx_hash,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    event_index,
    contract_address AS bridge_address,
    event_name,
    depositor,
    depositor AS sender,
    origin_from_address AS receiver,
    mint_recipient AS destination_chain_receiver,
    chain AS destination_chain,
    destination_domain AS destination_chain_id,
    burn_token AS token_address,
    burn_amount AS amount_unadj,
    protocol,
    version,
    platform,
    _log_id,
    e.modified_timestamp
FROM
    base_evt e
    LEFT JOIN {{ ref('silver_bridge__cctp_chain_id_seed') }}
    d
    ON domain = destination_domain
