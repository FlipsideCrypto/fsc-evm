{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_dex','defi','dex','curated']
) }}

WITH contract_mapping AS (
    {{ curated_contract_mapping(
        vars.CURATED_DEFI_DEX_SWAPS_CONTRACT_MAPPING
    ) }}
    WHERE
        protocol = 'synthetix'
),
swaps AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        l.contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS sender,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS amount_in_unadj,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: INTEGER AS amount_out_unadj,
        REGEXP_REPLACE(
            utils.udf_hex_to_string(
                segmented_data [0] :: STRING
            ),
            '[^a-zA-Z0-9]+'
        ) AS symbol_in,
        REGEXP_REPLACE(
            utils.udf_hex_to_string(
                segmented_data [2] :: STRING
            ),
            '[^a-zA-Z0-9]+'
        ) AS symbol_out,
        CONCAT('0x', SUBSTR(segmented_data [4] :: STRING, 25, 40)) AS tx_to,
        event_index,
        m.protocol,
        m.version,
        m.type,
        CONCAT(
            m.protocol,
            '-',
            m.version
        ) AS platform,
        'SynthExchange' AS event_name,
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
        topics [0] = '0x65b6972c94204d84cffd3a95615743e31270f04fdf251f3dccc705cfbad44776'
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
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_index,
    event_name,
    sender,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    tx_to,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp
FROM
    swaps s
    LEFT JOIN (
        SELECT
            synth_symbol AS synth_symbol_in,
            synth_proxy_address AS token_in,
            decimals AS decimals_in,
            blockchain
        FROM
            {{ ref('silver_dex__synthetix_synths_20230404') }}
    ) sc1
    ON sc1.synth_symbol_in = s.symbol_in
    AND sc1.blockchain = '{{ vars.GLOBAL_PROJECT_NAME }}'
    LEFT JOIN (
        SELECT
            synth_symbol AS synth_symbol_out,
            synth_proxy_address AS token_out,
            decimals AS decimals_out,
            blockchain
        FROM
            {{ ref('silver_dex__synthetix_synths_20230404') }}
    ) sc2
    ON sc2.synth_symbol_out = s.symbol_out
    AND sc2.blockchain = '{{ vars.GLOBAL_PROJECT_NAME }}'
