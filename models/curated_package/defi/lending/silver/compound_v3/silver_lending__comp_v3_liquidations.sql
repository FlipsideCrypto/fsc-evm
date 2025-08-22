{# Get variables #}
{% set vars = return_vars() %}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated','compound','compound_v3']
) }}

WITH comp_assets AS (

    SELECT
        compound_market_address,
        compound_market_name,
        compound_market_symbol,
        compound_market_decimals,
        underlying_asset_address,
        underlying_asset_name,
        protocol,
        version,
        platform
    FROM
        {{ ref('silver_lending__comp_v3_asset_details') }}
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
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS collateral_token,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS absorber,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS borrower,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS collateral_absorbed,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS usd_value,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        l.modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
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
        MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
AND l.modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
{% endif %}
),
repayments AS (
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
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS absorber,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS collateral_token,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS repaid_amount_unadj,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        l.modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
    WHERE
        topics [0] = '0x428a71022c65d48a5617ad1aa0b2ec7f865096caee9b5cd593fe1d83f01e36ca' --BuyCollateral
        AND l.contract_address IN (
            SELECT
                DISTINCT(compound_market_address)
            FROM
                comp_assets
        )
        AND tx_hash in (select tx_hash from liquidations)
        AND tx_succeeded
)
SELECT
    l.tx_hash,
    l.block_number,
    l.block_timestamp,
    l.event_index,
    l.origin_from_address,
    l.origin_to_address,
    l.origin_function_signature,
    l.contract_address,
    l.compound_market as protocol_market,
    l.absorber as liquidator,
    l.borrower,
    l.collateral_token,
    collateral_absorbed AS liquidated_amount_unadj,
    r.repaid_amount_unadj,
    A.underlying_asset_address AS debt_token,
    A.protocol,
    A.version,
    A.platform,
    l._log_id,
    l.modified_timestamp,
    'AbsorbCollateral' AS event_name
FROM
    liquidations l
    LEFT JOIN comp_assets A
    ON l.compound_market = A.compound_market_address 
    LEFT JOIN repayments r
    ON l.tx_hash = r.tx_hash
    AND l.absorber = r.absorber
    AND l.collateral_token = r.collateral_token
qualify(ROW_NUMBER() over(PARTITION BY l._log_id
ORDER BY
    l.modified_timestamp DESC)) = 1
