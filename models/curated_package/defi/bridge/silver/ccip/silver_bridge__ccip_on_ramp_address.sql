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
        protocol = 'chainlink_ccip'
)

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_name,
    decoded_log :destChainSelector :: STRING AS dest_chain_selector,
    chain_name,
    decoded_log :onRamp :: STRING AS on_ramp_address,
    m.protocol,
    m.version,
    m.type,
    CONCAT(
        m.protocol,
        '-',
        m.version
    ) AS platform,
    modified_timestamp
FROM
    {{ ref('core__ez_decoded_event_logs') }}
    l
    INNER JOIN contract_mapping m
    ON l.contract_address = m.contract_address
    INNER JOIN {{ ref('silver_bridge__ccip_chain_seed') }}
    ON dest_chain_selector = chain_selector
WHERE
    topic_0 = '0x1f7d0ec248b80e5c0dde0ee531c4fc8fdb6ce9a2b3d90f560c74acd6a7202f23' -- onrampset
    AND tx_succeeded
    AND event_removed = FALSE
    and block_timestamp::DATE >= '2023-01-01'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
{% endif %}
