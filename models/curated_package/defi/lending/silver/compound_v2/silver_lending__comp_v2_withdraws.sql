{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated','compound','compound_v2']
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
        protocol,
        version
    FROM
        {{ ref('silver_lending__comp_v2_asset_details') }}
),
comp_v2_fork_redemptions AS (
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
        CASE 
            WHEN topics[0]::STRING = '0x3f693fff038bb8a046aa76d9516190ac7444f7d69cf952c4cbdc086fdef2d6fc' THEN
                utils.udf_hex_to_int(segmented_data[3]::STRING)::INTEGER
            ELSE
                utils.udf_hex_to_int(segmented_data[1]::STRING)::INTEGER
        END AS received_amount_raw,
        CASE 
            WHEN topics[0]::STRING = '0x3f693fff038bb8a046aa76d9516190ac7444f7d69cf952c4cbdc086fdef2d6fc' THEN
                utils.udf_hex_to_int(segmented_data[2]::STRING)::INTEGER
            ELSE
                utils.udf_hex_to_int(segmented_data[3]::STRING)::INTEGER
        END AS redeemed_token_raw,
        CONCAT('0x', SUBSTR(segmented_data[0]::STRING, 25, 40)) AS depositor,
        modified_timestamp,
        CONCAT(tx_hash::STRING, '-', event_index::STRING) AS _log_id
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address IN (SELECT token_address FROM asset_details)
        AND topics [0] :: STRING IN ('0xe5b754fb1abb7f01b499791d0b820ae3b6af3424ac1c59768edb53f4ec31a929'
        ,'0xbd5034ffbd47e4e72a94baa2cdb74c6fad73cb3bcdc13036b72ec8306f5a7646'
        ,'0x3f693fff038bb8a046aa76d9516190ac7444f7d69cf952c4cbdc086fdef2d6fc')
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
        b.protocol_market,
        b.depositor,
        b.received_amount_raw,
        C.underlying_asset_address AS token_address,
        C.protocol,
        C.version,
        C.protocol || '-' || C.version as platform,
        b._log_id,
        b.modified_timestamp
    FROM
        comp_v2_fork_redemptions b
        LEFT JOIN asset_details C
        ON b.protocol_market = C.token_address
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
    protocol_market,
    depositor,
    token_address,
    received_amount_raw AS amount_unadj,
    platform,
    protocol,
    version,
    modified_timestamp as _inserted_timestamp,
    SYSDATE() as modified_timestamp,
    SYSDATE() as inserted_timestamp,
    _log_id,
    'Redeem' AS event_name
FROM
    comp_v2_fork_combine qualify(ROW_NUMBER() over(PARTITION BY _log_id ORDER BY modified_timestamp DESC)) = 1
