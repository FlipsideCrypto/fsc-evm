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

WITH unwrap_token AS (

    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        tx_hash,
        event_index,
        'UnwrapToken' AS event_name,
        'core_bridge' AS protocol,
        'v1' AS version,
        'native' AS type,
        CONCAT(
            protocol,
            '-',
            version
        ) AS platform,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS local_token,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS remote_token,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [2] :: STRING)) AS remote_chain_id,
        CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40)) AS to_address,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [4] :: STRING)) AS amount_unadj,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address = '0xa4218e1f39da4aadac971066458db56e901bcbde'
        AND topic_0 = '0x3b661011d9e0ff8f0dc432bac4ed79eabf70cf52596ed9de985810ef36689e9e'
        AND tx_succeeded
        AND '{{ vars.GLOBAL_PROJECT_NAME }}' = 'core'

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
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_hash,
    event_index,
    contract_address AS bridge_address,
    contract_address,
    event_name,
    protocol,
    version,
    type,
    platform,
    origin_from_address AS sender,
    to_address AS receiver,
    receiver AS destination_chain_receiver,
    remote_chain_id :: STRING AS destination_chain_id,
    s.chain_name AS destination_chain,
    local_token AS token_address,
    amount_unadj,
    _log_id,
    modified_timestamp
FROM
    unwrap_token
    LEFT JOIN {{ ref('silver_bridge__stargate_chain_id_seed') }}
    s
    ON unwrap_token.remote_chain_id :: STRING = s.chain_id :: STRING
