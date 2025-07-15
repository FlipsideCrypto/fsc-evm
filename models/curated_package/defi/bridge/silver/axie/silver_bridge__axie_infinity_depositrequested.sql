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
        protocol = 'axie_infinity'
        AND version = 'v2'
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
            decoded_log :"receipt" :"info" :"quantity" :: STRING
        ) AS amount,
        TRY_TO_NUMBER(
            decoded_log :"receipt" :"ronin" :"chainId" :: STRING
        ) AS chainId,
        decoded_log :"receipt" :"mainchain" :"addr" :: STRING AS sender,
        decoded_log :"receipt" :"ronin" :"addr" :: STRING AS receiver,
        decoded_log :"receipt" :"mainchain" :"tokenAddr" :: STRING AS token_address,
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
        topics [0] :: STRING = '0xd7b25068d9dc8d00765254cfb7f5070f98d263c8d68931d937c7362fa738048b' -- DepositRequested
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
    contract_address AS bridge_address,
    NAME AS platform,
    sender,
    receiver,
    receiver AS destination_chain_receiver,
    amount,
    chainId AS destination_chain_id,
    token_address,
    protocol,
    version,
    platform,
    _log_id,
    modified_timestamp
FROM
    base_evt