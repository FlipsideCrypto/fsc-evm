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
        protocol = 'hyperliquid'
),
mapping AS (
    SELECT
        c1.contract_address AS bridge_address,
        c2.contract_address AS token_address,
        version
    FROM
        contract_mapping c1
        INNER JOIN contract_mapping c2 USING (version)
    WHERE
        c1.type = 'bridge'
        AND c2.type = 'token'
)
SELECT
    block_number,
    block_timestamp,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_hash,
    event_index,
    'Transfer' AS event_name,
    to_address AS bridge_address,
    'hyperliquid' AS protocol,
    version,
    'bridge' AS TYPE,
    CONCAT(
        protocol,
        '-',
        version
    ) AS platform,
    from_address AS sender,
    from_address AS receiver,
    from_address AS destination_chain_receiver,
    t.contract_address AS token_address,
    raw_amount AS amount_unadj,
    '42161' AS destination_chain_id,
    'arbitrum' AS destination_chain,
    CONCAT(
        tx_hash,
        '-',
        event_index
    ) AS _id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('core__ez_token_transfers') }}
    t
    INNER JOIN mapping m
    ON t.to_address = m.bridge_address
    AND t.contract_address = m.token_address
WHERE
    block_timestamp :: DATE >= '2023-02-01'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
{% endif %}
