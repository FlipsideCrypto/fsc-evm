{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated']
) }}

WITH comp_assets AS (

    SELECT
        compound_market_address,
        compound_market_name,
        compound_market_symbol,
        compound_market_decimals,
        underlying_asset_address,
        underlying_asset_name,
        underlying_asset_symbol,
        underlying_asset_decimals,
        protocol,
        version,
        platform
    FROM
        {{ ref('silver__comp_v3_asset_details') }}
),
liquidations AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        l.contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        l.contract_address AS compound_market,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS asset,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS absorber,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS borrower,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS collateral_absorbed,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS usd_value,
        origin_from_address AS depositor_address,
        C.token_name,
        C.token_symbol,
        C.token_decimals,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        l.modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON asset = C.contract_address
    WHERE
        topics [0] = '0x9850ab1af75177e4a9201c65a2cf7976d5d28e40ef63494b44366f86b2f9412e' --AbsorbCollateral
        AND l.contract_address IN (
            SELECT
                DISTINCT(compound_market_address)
            FROM
                comp_assets
        )
        AND tx_succeeded

{% if is_incremental() %}
AND l.modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND l.modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    l.contract_address,
    compound_market as protocol_market,
    absorber as liquidator,
    borrower,
    depositor_address,
    asset AS collateral_token,
    token_symbol AS collateral_token_symbol,
    collateral_absorbed AS liquidated_amount_unadj,
    collateral_absorbed / pow(
        10,
        token_decimals
    ) AS liquidated_amount,
    null as repaid_amount_unadj,
    null as repaid_amount,
    A.underlying_asset_address AS debt_token,
    A.underlying_asset_symbol AS debt_token_symbol,
    A.protocol,
    A.version,
    A.platform,
    l._log_id,
    l._inserted_timestamp,
    'AbsorbCollateral' AS event_name
FROM
    liquidations l
    LEFT JOIN comp_assets A
    ON l.compound_market = A.compound_market_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
