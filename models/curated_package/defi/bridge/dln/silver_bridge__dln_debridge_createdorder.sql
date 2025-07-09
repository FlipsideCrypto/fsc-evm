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
        protocol = 'dln_debridge'
),
WITH base_evt AS (

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
        DATA,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [24] :: STRING, 1, 40)) AS token_address,
        decoded_log :"affiliateFee" :: STRING AS affiliateFee,
        decoded_log :"metadata" :: STRING AS metadata,
        TRY_TO_NUMBER(
            decoded_log :"nativeFixFee" :: STRING
        ) AS nativeFixFee,
        decoded_log :"order" AS order_obj,
        decoded_log :"order" :"allowedCancelBeneficiarySrc" :: STRING AS allowedCancelBeneficiarySrc,
        decoded_log :"order" :"allowedTakerDst" :: STRING AS allowedTakerDst,
        decoded_log :"order" :"externalCall" :: STRING AS externalCall,
        TRY_TO_NUMBER(
            decoded_log :"order" :"giveAmount" :: STRING
        ) AS giveAmount,
        TRY_TO_NUMBER(
            decoded_log :"order" :"giveChainId" :: STRING
        ) AS giveChainId,
        decoded_log :"order" :"givePatchAuthoritySrc" :: STRING AS givePatchAuthoritySrc,
        decoded_log :"order" :"giveTokenAddress" :: STRING AS giveTokenAddress,
        TRY_TO_NUMBER(
            decoded_log :"order" :"makerOrderNonce" :: STRING
        ) AS makerOrderNonce,
        decoded_log :"order" :"makerSrc" :: STRING AS makerSrc,
        decoded_log :"order" :"orderAuthorityAddressDst" :: STRING AS orderAuthorityAddressDst,
        CONCAT('0x', LEFT(segmented_data [28] :: STRING, 40)) AS receiverDst,
        TRY_TO_NUMBER(
            decoded_log :"order" :"takeAmount" :: STRING
        ) AS takeAmount,
        TRY_TO_NUMBER(
            decoded_log :"order" :"takeChainId" :: STRING
        ) AS takeChainId,
        decoded_log :"order" :"takeTokenAddress" :: STRING AS takeTokenAddress,
        decoded_log :"orderId" :: STRING AS orderId,
        TRY_TO_NUMBER(
            decoded_log :"percentFee" :: STRING
        ) AS percentFee,
        TRY_TO_NUMBER(
            decoded_log :"referralCode" :: STRING
        ) AS referralCode,
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
        topics [0] :: STRING = '0xfc8703fd57380f9dd234a89dce51333782d49c5902f307b02f03e014d18fe471' --CreatedOrder
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
    tx_succeeded,
    contract_address AS bridge_address,
    origin_from_address AS sender,
    receiverDst AS receiver,
    CASE
        WHEN takeChainId :: STRING = '7565164' THEN utils.udf_hex_to_base58(CONCAT('0x', segmented_data [28] :: STRING))
        ELSE receiverDst
    END AS destination_chain_receiver,
    giveAmount AS amount,
    takeChainId AS destination_chain_id,
    CASE
        WHEN destination_chain_id :: STRING = '7565164' THEN 'solana'
        ELSE NULL
    END AS destination_chain,
    CASE
        WHEN token_address = '0x0000000000000000000000000000000000000000' THEN '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}'
        ELSE token_address
    END AS token_address,
    decoded_log,
    order_obj,
    protocol,
    version,
    platform,
    _log_id,
    modified_timestamp
FROM
    base_evt
