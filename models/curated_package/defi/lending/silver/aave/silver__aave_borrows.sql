{# Get variables #}
{% set vars = return_vars() %}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated','aave','borrows']
) }}

WITH --borrows from Aave LendingPool contracts
token_meta AS (

    SELECT
    atoken_created_block,
    version_pool,
    treasury_address,
    atoken_symbol,
    atoken_address,
    token_stable_debt_address,
    token_variable_debt_address,
    atoken_decimals,
    atoken_version,
    atoken_name,
    underlying_symbol,
    underlying_address,
    underlying_decimals,
    underlying_name,
    protocol,
    version,
    modified_timestamp,
    _log_id
    FROM
        {{ ref('silver__aave_tokens') }}
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
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS market,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS onBehalfOf,
        utils.udf_hex_to_int(
            topics [3] :: STRING
        ) :: INTEGER AS refferal,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS userAddress,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS borrow_quantity,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: INTEGER AS borrow_rate_mode,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: INTEGER AS borrowrate,
        origin_from_address AS borrower_address,
        COALESCE(
            contract_address,
            origin_to_address
        ) AS lending_pool_contract,
        modified_timestamp,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING IN (
            '0xc6a898309e823ee50bac64e45ca8adba6690e99e7841c45d754e2a38e9019d9b',
            '0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0'
        )


{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
{% endif %}
AND lending_pool_contract IN (
    SELECT
        DISTINCT(version_pool)
    FROM
        token_meta
)
AND tx_succeeded
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    borrower_address AS borrower,
    t.atoken_address AS protocol_market,
    t.underlying_address AS token_address,
    t.underlying_symbol AS token_symbol,
    borrow_quantity AS amount_unadj,
    borrow_quantity / pow(
        10,
        t.underlying_decimals
    ) AS amount,
    CASE
        WHEN borrow_rate_mode = 2 THEN 'Variable Rate'
        ELSE 'Stable Rate'
    END AS borrow_rate_mode,
    lending_pool_contract,
    t.protocol || '-' || t.version AS platform,
    t.protocol,
    t.version,
    b._log_id,
    b.modified_timestamp,
    'Borrow' AS event_name
FROM
    borrow b
    LEFT JOIN token_meta t
    ON b.market = t.underlying_address
    and b.lending_pool_contract = t.version_pool qualify(ROW_NUMBER() over(PARTITION BY b._log_id
ORDER BY
    b.modified_timestamp DESC)) = 1
