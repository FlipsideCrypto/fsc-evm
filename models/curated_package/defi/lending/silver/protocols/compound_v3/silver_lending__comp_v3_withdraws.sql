{# Get variables #}
{% set vars = return_vars() %}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated','comp_v3']
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
withdraw AS (
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
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS token_address,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS withdraw_amount,
        origin_from_address AS depositor_address,
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
        topics [0] = '0xd6d480d5b3068db003533b170d67561494d72e3bf9fa40a266471351ebba9e16' --WithdrawCollateral
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
borrow AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        contract_address AS asset,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS src_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS to_address,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS borrow_amount,
        origin_from_address AS borrower_address,
        C.compound_market_name AS NAME,
        C.compound_market_symbol AS symbol,
        C.compound_market_decimals AS decimals,
        C.underlying_asset_address,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        LEFT JOIN comp_assets C
        ON asset = C.compound_market_address
    WHERE
        topics [0] = '0x9b1bfa7fa9ee420a16e124f794c35ac9f90472acc99140eb2f6447c714cad8eb' --withdrawl
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
borrows_checks as (
select
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    to_address,
    amount
from
   {{ ref('core__ez_token_transfers') }}
where 
    tx_hash in (select distinct tx_hash from borrow)
    and to_address = '0x0000000000000000000000000000000000000000'
),
final_base_wd as (
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    w.asset AS protocol_market,
    borrower_address AS depositor,
    w.underlying_asset_address AS token_address,
    borrow_amount AS amount_unadj,
    w.symbol AS itoken_symbol,
    A.protocol,
    A.version,
    A.platform,
    _log_id,
    modified_timestamp,
    'Withdraw' AS event_name
FROM
    borrow w
    LEFT JOIN comp_assets A
    ON w.asset = A.compound_market_address
    LEFT JOIN borrows_checks B
    ON w.tx_hash = B.tx_hash
    and w.borrower_address = b.from_address
    and w.asset=b.contract_address
    WHERE b.to_address IS NOT NULL
),
withdraw_union as (
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    compound_market as protocol_market,
    depositor_address as depositor,
    w.token_address,
    withdraw_amount AS amount_unadj,
    A.protocol,
    A.version,
    A.platform,
    _log_id,
    modified_timestamp,
    'WithdrawCollateral' AS event_name
FROM
    withdraw w
    LEFT JOIN comp_assets A
    ON w.compound_market = A.compound_market_address
UNION ALL
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    protocol_market,
    depositor,
    w.underlying_asset_address AS token_address,
    borrow_amount AS amount_unadj,
    w.symbol AS itoken_symbol,
    A.protocol,
    A.version,
    A.platform,
    _log_id,
    modified_timestamp,
    'Withdraw' AS event_name
FROM
    final_base_wd w
    LEFT JOIN comp_assets A
    ON w.protocol_market = A.compound_market_address
)
SELECT
    *
FROM
    withdraw_union
QUALIFY ROW_NUMBER() OVER (PARTITION BY _log_id ORDER BY modified_timestamp DESC) = 1