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
        protocol = 'across'
        AND version = 'v3'
),
base_evt AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        l.contract_address,
        event_index,
        topic_0,
        CASE
            WHEN topic_0 = '0x32ed1a409ef04c7b0227189c3a103dc5ac10e775a15b785dcc510201f7c25ad3' THEN 'FundsDeposited'
            WHEN topic_0 = '0xa123dc29aebf7d0c3322c8eeb5b999e859f39937950ed31056532713d0de396f' THEN 'V3FundsDeposited'
        END AS event_name,
        topics,
        DATA,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                topics [1] :: STRING
            )
        ) AS destinationChainId,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                topics [2] :: STRING
            )
        ) AS depositId,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS depositor,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS inputToken,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS outputToken,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            )
        ) AS inputAmount,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [3] :: STRING
            )
        ) AS outputAmount,
        TRY_TO_TIMESTAMP(
            utils.udf_hex_to_int(
                segmented_data [4] :: STRING
            )
        ) AS quoteTimestamp,
        TRY_TO_TIMESTAMP(
            utils.udf_hex_to_int(
                segmented_data [5] :: STRING
            )
        ) AS fillDeadline,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [6] :: STRING
            )
        ) AS exclusivityDeadline,
        CONCAT('0x', SUBSTR(segmented_data [7] :: STRING, 25, 40)) AS recipient,
        CONCAT('0x', SUBSTR(segmented_data [8] :: STRING, 25, 40)) AS exclusiveRelayer,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [9] :: STRING
            )
        ) AS relayerFeePct,
        segmented_data [10] :: STRING AS message,
        event_removed,
        m.protocol,
        m.version,
        CONCAT(m.protocol, '-', m.version) AS platform,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        INNER JOIN contract_mapping m
        ON l.contract_address = m.contract_address
    WHERE
        topic_0 IN (
            '0x32ed1a409ef04c7b0227189c3a103dc5ac10e775a15b785dcc510201f7c25ad3',
            '0xa123dc29aebf7d0c3322c8eeb5b999e859f39937950ed31056532713d0de396f'
        )
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
    contract_address AS bridge_address,
    depositor AS sender,
    recipient AS receiver,
    recipient AS destination_chain_receiver,
    destinationChainId AS destination_chain_id,
    inputAmount AS amount,
    inputToken AS token_address,
    depositId AS deposit_id,
    message,
    quoteTimestamp AS quote_timestamp,
    relayerFeePct AS relayer_fee_pct,
    exclusiveRelayer AS exclusive_relayer,
    exclusivityDeadline AS exclusivity_deadline,
    fillDeadline AS fill_deadline,
    outputAmount AS output_amount,
    outputToken AS output_token,
    protocol,
    version,
    platform,
    _log_id,
    modified_timestamp
FROM
    base_evt
