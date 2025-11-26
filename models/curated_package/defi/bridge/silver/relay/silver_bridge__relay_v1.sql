{# Get variables #}
{% set vars = return_vars() %}
{# Log configuration details #}
{{ log_model_details() }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    tags = ['silver_bridge','defi','bridge','curated']
) }}

WITH contract_mapping AS (
    {{ curated_contract_mapping(
        vars.CURATED_DEFI_BRIDGE_CONTRACT_MAPPING
    ) }}
    WHERE
        protocol = 'relay'
        AND version = 'v1'
)
SELECT
    in_txs_block_number AS block_number,
    created_timestamp AS block_timestamp,
    NULL AS origin_from_address,
    NULL AS origin_to_address,
    NULL AS origin_function_signature,
    in_txs_tx_hash AS tx_hash,
    NULL AS event_index,
    '0xf70da97812cb96acdf810712aa562db8dfa3dbef' AS bridge_address,
    NULL AS event_name,
    sender,
    receiver,
    receiver AS destination_chain_receiver,
    destination_chain_id :: STRING AS destination_chain_id,
    standardized_name AS destination_chain,
    source_currency_address AS token_address,
    NULL AS token_symbol,
    source_amount_raw AS amount_unadj,
    'relay-v1' AS platform,
    'relay' AS protocol,
    'v1' AS version,
    TYPE,
    id AS _id,
    modified_timestamp AS _inserted_timestamp
FROM
    {{ source(
        'external_silver',
        'relay_bridge'
    ) }}
    r
    INNER JOIN contract_mapping C
    ON C.contract_address = r.chain_id_from_request
    LEFT JOIN {{ source('external_bronze', 'relay_bridge_chainid_seed') }}
    s
    ON destination_chain_id = s.chain_id
WHERE
    chain_id_from_request = source_chain_id

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
{% endif %}
