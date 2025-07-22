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
-- add the collateral liquidated here
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
comp_v2_fork_liquidations AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS borrower,
        contract_address AS token,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS liquidator,
        utils.udf_hex_to_int(segmented_data [4] :: STRING) :: INTEGER AS seizeTokens_raw,
        utils.udf_hex_to_int(segmented_data [2] :: STRING) :: INTEGER AS repayAmount_raw,
        CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40)) AS tokenCollateral,
        modified_timestamp,
        CONCAT(tx_hash :: STRING, '-', event_index :: STRING) AS _log_id
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address IN (SELECT token_address FROM asset_details)
        AND topics [0] :: STRING = '0x298637f684da70674f26509b10f07ec2fbc77a335ab1e7d6215a4b2484d8bb52'
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
liquidation_union AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        l.event_index,
        l.origin_from_address,
        l.origin_to_address,
        l.origin_function_signature,
        l.contract_address,
        l.borrower,
        l.token,
        asd1.token_symbol AS token_symbol,
        l.liquidator,
        l.seizeTokens_raw / pow(10, asd2.token_decimals) AS tokens_seized,
        l.tokenCollateral AS protocol_market,
        asd2.token_symbol AS collateral_token_symbol,
        asd2.underlying_asset_address AS collateral_token,
        asd2.underlying_symbol AS collateral_symbol,
        l.repayAmount_raw AS amount_unadj,
        l.repayAmount_raw / pow(10, asd1.underlying_decimals) AS amount,
        asd1.underlying_decimals,
        asd1.underlying_asset_address AS debt_asset,
        asd1.underlying_symbol AS debt_asset_symbol,
        asd1.protocol,
        asd1.version,
        asd1.protocol || '-' || asd1.version as platform,
        l.modified_timestamp,
        l._log_id
    FROM
        comp_v2_fork_liquidations l
        LEFT JOIN asset_details asd1
        ON l.token = asd1.token_address
        LEFT JOIN asset_details asd2
        ON l.tokenCollateral = asd2.token_address
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
        b.borrower,
        b.token,
        C.token_symbol,
        b.liquidator,
        b.seizeTokens_raw / pow(10, C.token_decimals) AS tokens_seized,
        b.tokenCollateral AS protocol_market,
        C.token_symbol AS collateral_token_symbol,
        C.underlying_asset_address AS collateral_token,
        C.underlying_symbol AS collateral_symbol,
        b.repayAmount_raw AS amount_unadj,
        b.repayAmount_raw / pow(10, C.underlying_decimals) AS amount,
        C.underlying_decimals,
        C.underlying_asset_address AS debt_asset,
        C.underlying_symbol AS debt_asset_symbol,
        C.protocol,
        C.version,
        C.protocol || '-' || C.version as platform,
        b.modified_timestamp,
        b._log_id
    FROM
        {{this}} b
        LEFT JOIN asset_details C
        ON b.token = C.token_address
    WHERE
        (b.token_symbol IS NULL and C.token_symbol is not null)
        OR (b.collateral_token_symbol IS NULL and C.underlying_symbol is not null)
{% endif %}
)
SELECT
    *
FROM
    liquidation_union qualify(ROW_NUMBER() over(PARTITION BY _log_id ORDER BY modified_timestamp DESC)) = 1
