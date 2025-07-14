{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 
        'database_tags':{
            'table':{
                'PROTOCOL': 'ACROSS, ALLBRIDGE, AXELAR, CELER, CBRIDGE, DLN, DEBRIDGE, EYWA, HOP, MESON, MULTICHAIN, STARGATE, SYMBIOSIS, SYNAPSE, WORMHOLE, CCIP',
                'PURPOSE': 'BRIDGE'
        } } },
    tags = ['gold','defi','bridge','curated','ez']
) }}

SELECT
    block_number,
    block_timestamp,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_hash,
    event_index,
    bridge_address,
    event_name,
    platform,
    protocol,
    version,
    sender,
    receiver,
    destination_chain_receiver,
    COALESCE(
        c.standardized_name,
        b.destination_chain
    ) AS destination_chain,
    destination_chain_id,
    token_address,
    token_symbol,
    amount_unadj,
    amount,
    ROUND(
        CASE
            WHEN amount_usd < 1e+15 THEN amount_usd
            ELSE NULL
        END,
        2
    ) AS amount_usd,
    token_is_verified,
    complete_bridge_activity_id AS ez_bridge_activity_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver_bridge__complete_bridge_activity') }}
    b
    LEFT JOIN {{ ref('silver_bridge__standard_chain_seed') }} C
    ON b.destination_chain = C.variation
