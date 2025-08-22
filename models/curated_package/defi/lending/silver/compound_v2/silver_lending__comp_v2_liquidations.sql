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
        contract_address AS protocol_market,
        asd2.underlying_asset_address AS collateral_token,
        asd1.underlying_asset_address AS debt_token,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS liquidator,
        utils.udf_hex_to_int(segmented_data [4] :: STRING) :: INTEGER AS seizeTokens_raw,
        utils.udf_hex_to_int(segmented_data [2] :: STRING) :: INTEGER AS repayAmount_raw,
        CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40)) AS tokenCollateral,
        asd1.protocol,
        asd1.version,
        asd1.protocol || '-' || asd1.version as platform,
        modified_timestamp,
        CONCAT(tx_hash :: STRING, '-', event_index :: STRING) AS _log_id
    FROM
        {{ ref('core__fact_event_logs') }}
        LEFT JOIN asset_details asd1
        ON contract_address = asd1.token_address
        LEFT JOIN asset_details asd2
        ON tokenCollateral = asd2.token_address
    WHERE
        topics [0] :: STRING = '0x298637f684da70674f26509b10f07ec2fbc77a335ab1e7d6215a4b2484d8bb52'
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
transfers AS (
    SELECT
        block_number, 
        block_timestamp, 
        tx_hash, 
        tx_position, 
        event_index, 
        from_address, 
        to_address, 
        contract_address, 
        name, 
        symbol, 
        decimals, 
        raw_amount,
        amount, 
        amount_usd, 
        origin_function_signature, 
        origin_from_address, 
        origin_to_address, 
        ez_token_transfers_id, 
        inserted_timestamp, 
        modified_timestamp
    FROM
        {{ ref('core__ez_token_transfers') }}
    WHERE
        tx_hash IN (SELECT tx_hash FROM comp_v2_fork_liquidations)
),
transfers_join AS (
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
        l.liquidator,
        l.tokenCollateral AS protocol_market,
        t1.contract_address AS collateral_token,
        t1.raw_amount AS liquidated_amount_unadj,
        t2.contract_address AS debt_token,
        t2.raw_amount AS repaid_amount_unadj,
        protocol,
        version,
        platform,
        l.modified_timestamp,
        l._log_id
    FROM
        comp_v2_fork_liquidations l
        LEFT JOIN transfers t1
        ON l.tx_hash = t1.tx_hash
        AND t1.to_address = l.liquidator
        AND t1.contract_address = l.collateral_token
        LEFT JOIN transfers t2
        ON l.tx_hash = t2.tx_hash
        AND t2.from_address = l.liquidator
        AND t2.contract_address = l.debt_token
)
{% if is_incremental() %}
,broken_records as (
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
        l.liquidator,
        l.protocol_market,
        l.collateral_token,
        l.liquidated_amount_unadj,
        l.debt_token,
        l.repaid_amount_unadj,
        protocol,
        version,
        platform,
        l.modified_timestamp,
        l._log_id
    FROM
        {{this}} l
        INNER JOIN transfers t1
        ON l.tx_hash = t1.tx_hash
        AND t1.to_address = l.liquidator
        AND t1.contract_address = l.collateral_token
        INNER JOIN transfers t2
        ON l.tx_hash = t2.tx_hash
        AND t2.from_address = l.liquidator
        AND t2.contract_address = l.debt_token
        WHERE (
        ((l.collateral_token_symbol IS NULL OR l.collateral_token_symbol = '') AND t1.symbol IS NOT NULL)
        OR ((l.debt_token_symbol IS NULL OR l.debt_token_symbol = '') AND t2.symbol IS NOT NULL)
        OR (l.liquidated_amount IS NULL AND t1.amount IS NOT NULL)
        OR (l.repaid_amount IS NULL AND t2.amount IS NOT NULL)
    ) 
)
{% endif %}
, liquidation_union AS (
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    borrower,
    liquidator,
    protocol_market,
    collateral_token,
    liquidated_amount_unadj,
    debt_token,
    repaid_amount_unadj,
    protocol,
    version,
    platform,
    modified_timestamp,
    _log_id
FROM
    transfers_join
{% if is_incremental() %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    borrower,
    liquidator,
    protocol_market,
    collateral_token,
    liquidated_amount_unadj,
    debt_token,
    repaid_amount_unadj,
    protocol,
    version,
    platform,
    modified_timestamp,
    _log_id
FROM
    broken_records
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
    borrower,
    liquidator,
    protocol_market,
    collateral_token,
    liquidated_amount_unadj,
    debt_token,
    repaid_amount_unadj,
    protocol,
    version,
    platform,
    _log_id,
    modified_timestamp as _inserted_timestamp,
    SYSDATE() as modified_timestamp,
    SYSDATE() as inserted_timestamp,
    'LiquidateBorrow' AS event_name
FROM
    liquidation_union qualify(ROW_NUMBER() over(PARTITION BY _log_id ORDER BY modified_timestamp DESC)) = 1
