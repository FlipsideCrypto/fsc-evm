{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated']
) }}
-- pull all token addresses and corresponding name
WITH asset_details AS (
    SELECT
        token_address,
        token_symbol,
        token_name,
        token_decimals,
        underlying_asset_address,
        underlying_name,
        underlying_symbol,
        underlying_decimals,
        protocol,
        version
    FROM
        {{ ref('silver__comp_v2_fork_asset_details') }}
),
comp_v2_fork_deposits AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        contract_address AS protocol_market,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        utils.udf_hex_to_int(segmented_data [2] :: STRING) :: INTEGER AS minttokens_raw,
        utils.udf_hex_to_int(segmented_data [1] :: STRING) :: INTEGER AS mintAmount_raw,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS supplier,
        modified_timestamp,
        CONCAT(tx_hash :: STRING, '-', event_index :: STRING) AS _log_id
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address IN (SELECT protocol_market FROM asset_details)
        AND topics [0] :: STRING = '0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f'
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
),
comp_v2_fork_combine AS (
    SELECT
        b.block_number,
        b.block_timestamp,
        b.tx_hash,
        b.event_index,
        b.origin_from_address,
        b.origin_to_address,
        b.origin_function_signature,
        b.contract_address,
        b.supplier,
        b.minttokens_raw,
        b.mintAmount_raw,
        b.protocol_market,
        C.underlying_asset_address AS token_address,
        C.underlying_symbol AS token_symbol,
        C.token_symbol,
        C.token_decimals,
        C.underlying_decimals,
        C.protocol,
        C.version,
        C.protocol || '-' || C.version as platform,
        b._log_id,
        b.modified_timestamp
    FROM
        comp_v2_fork_deposits b
        LEFT JOIN asset_details C
        ON b.protocol_market = C.token_address
{% if is_incremental() %}
    UNION ALL
    SELECT
        b.block_number,
        b.block_timestamp,
        b.tx_hash,
        b.event_index,
        b.origin_from_address,
        b.origin_to_address,
        b.origin_function_signature,
        b.contract_address,
        b.supplier,
        b.issued_tokens AS minttokens_raw,
        b.amount_unadj AS mintAmount_raw,
        b.protocol_market,
        C.underlying_asset_address AS token_address,
        C.underlying_symbol AS token_symbol,
        C.underlying_decimals,
        C.protocol,
        C.version,
        C.protocol || '-' || C.version as platform,
        b._log_id,
        b.modified_timestamp
    FROM
        {{this}} b
        LEFT JOIN asset_details C
        ON b.protocol_market = C.token_address
    WHERE
        (b.protocol_market IS NULL and C.token_symbol is not null)
        OR (b.token_symbol IS NULL and C.underlying_symbol is not null)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    supplier,
    protocol_market,
    token_address,
    token_symbol,
    mintAmount_raw AS amount_unadj,
    mintAmount_raw / pow(10, underlying_decimals) AS amount,
    platform,
    protocol,
    version,
    modified_timestamp,
    _log_id
FROM
    comp_v2_fork_combine qualify(ROW_NUMBER() over(PARTITION BY _log_id ORDER BY modified_timestamp DESC)) = 1
