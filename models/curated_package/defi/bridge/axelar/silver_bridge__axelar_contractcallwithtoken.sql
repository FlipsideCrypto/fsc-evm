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
        protocol = 'axelar'
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
        topics [0] :: STRING AS topic_0,
        event_name,
        TRY_TO_NUMBER(
            decoded_log :"amount" :: STRING
        ) AS amount,
        decoded_log :"destinationChain" :: STRING AS destinationChain,
        LOWER(
            decoded_log :"destinationContractAddress" :: STRING
        ) AS destinationContractAddress,
        decoded_log :"payload" :: STRING AS payload,
        origin_from_address AS recipient,
        decoded_log :"payloadHash" :: STRING AS payloadHash,
        decoded_log :"sender" :: STRING AS sender,
        decoded_log :"symbol" :: STRING AS symbol,
        decoded_log,
        event_removed,
        tx_succeeded,
        m.protocol,
        m.version,
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
        topics [0] :: STRING = '0x7e50569d26be643bda7757722291ec66b1be66d8283474ae3fab5a98f878a7a2'
        AND tx_succeeded
        AND m.type = 'gateway'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'

{% endif %}
),
native_gas_paid AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        l.contract_address,
        event_index,
        topics [0] :: STRING AS topic_0,
        event_name,
        TRY_TO_NUMBER(
            decoded_log :"amount" :: STRING
        ) AS amount,
        decoded_log :"destinationChain" :: STRING AS destinationChain,
        LOWER(
            decoded_log :"destinationAddress" :: STRING
        ) AS destinationAddress,
        TRY_TO_NUMBER(
            decoded_log :"gasFeeAmount" :: STRING
        ) AS gasFeeAmount,
        decoded_log :"payloadHash" :: STRING AS payloadHash,
        decoded_log :"refundAddress" :: STRING AS refundAddress,
        decoded_log :"sourceAddress" :: STRING AS sourceAddress,
        decoded_log :"symbol" :: STRING AS symbol,
        decoded_log,
        event_removed,
        tx_succeeded,
        m.protocol,
        m.version,
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
        topics [0] :: STRING = '0x999d431b58761213cf53af96262b67a069cbd963499fd8effd1e21556217b841'
        AND tx_succeeded
        AND m.type = 'gas_service'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'

{% endif %}
),
transfers AS (
    SELECT
        block_number,
        tx_hash,
        event_index,
        contract_address AS token_address,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__ez_token_transfers') }}
    WHERE
        from_address = '0xce16f69375520ab01377ce7b88f5ba8c48f8d666'
        AND to_address IN (
            SELECT contract_address
            FROM contract_mapping
            WHERE type IN ('gateway', 'burn')
        )

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
{% endif %}
),
FINAL AS (
    SELECT
        b.block_number,
        b.block_timestamp,
        b.origin_function_signature,
        b.origin_from_address,
        b.origin_to_address,
        b.tx_hash,
        b.event_index,
        b.topic_0,
        b.event_name,
        b.event_removed,
        b.tx_succeeded,
        b.contract_address AS bridge_address,
        b.origin_from_address AS sender,
        CASE
            WHEN b.recipient = '0x0000000000000000000000000000000000000000' THEN refundAddress
            ELSE b.recipient
        END AS receiver,
        CASE
            WHEN LOWER(
                b.destinationChain
            ) = 'avalanche' THEN 'avalanche c-chain'
            WHEN LOWER(
                b.destinationChain
            ) = 'binance' THEN 'bnb smart chain mainnet'
            WHEN LOWER(
                b.destinationChain
            ) = 'celo' THEN 'celo mainnet'
            WHEN LOWER(
                b.destinationChain
            ) = 'ethereum' THEN 'ethereum mainnet'
            WHEN LOWER(
                b.destinationChain
            ) = 'fantom' THEN 'fantom opera'
            WHEN LOWER(
                b.destinationChain
            ) = 'polygon' THEN 'polygon mainnet'
            ELSE LOWER(
                b.destinationChain
            )
        END AS destination_chain,
        b.destinationContractAddress AS destination_contract_address,
        CASE
            WHEN destination_chain IN (
                'arbitrum',
                'avalanche c-chain',
                'base',
                'bnb smart chain mainnet',
                'celo mainnet',
                'ethereum mainnet',
                'fantom opera',
                'filecoin',
                'kava',
                'linea',
                'mantle',
                'moonbeam',
                'optimism',
                'polygon mainnet',
                'scroll'
            ) THEN receiver
            ELSE destination_contract_address
        END AS destination_chain_receiver,
        b.amount,
        b.payload,
        b.payloadHash AS payload_hash,
        b.symbol AS token_symbol,
        t.token_address,
        b.platform,
        b.protocol,
        b.version,
        b._log_id,
        b.modified_timestamp
    FROM
        base_evt b
        INNER JOIN transfers t
        ON b.block_number = t.block_number
        AND b.tx_hash = t.tx_hash
        LEFT JOIN native_gas_paid n
        ON n.block_number = b.block_number
        AND n.tx_hash = b.tx_hash
)
SELECT
    *
FROM
    FINAL qualify (ROW_NUMBER() over (PARTITION BY _log_id
ORDER BY
    modified_timestamp DESC)) = 1
