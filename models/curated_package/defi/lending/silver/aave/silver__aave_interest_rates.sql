{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated','aave','aave']
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
reserve_data AS (
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
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS token_address,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS liquidity_rate,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS stable_borrow_rate,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: INTEGER AS variable_borrow_rate,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: INTEGER AS liquidity_index,
        utils.udf_hex_to_int(
            segmented_data [4] :: STRING
        ) :: INTEGER AS variable_borrow_index,
        modified_timestamp,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING = '0x804c9b842b2748a22bb64b345453a3de7ca54a6ca45ce00d415894979e22897a'

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
    r.tx_hash,
    r.block_number,
    r.block_timestamp,
    r.event_index,
    r.origin_from_address,
    r.origin_to_address,
    r.origin_function_signature,
    r.contract_address,
    r.token_address,
    t.underlying_symbol AS token_symbol,
    liquidity_rate / pow(10, 27) AS liquidity_rate,
    stable_borrow_rate / pow(10, 27) AS stable_borrow_rate,
    variable_borrow_rate / pow(10, 27) AS variable_borrow_rate,
    r.liquidity_index,
    r.variable_borrow_index,
    r.lending_pool_contract,
    t.protocol || '-' || t.version AS platform,
    t.protocol,
    t.version,
    b._log_id,
    b.modified_timestamp,
    'Borrow' AS event_name
FROM
    reserve_data r
    INNER JOIN token_meta t
    ON r.token_address = t.underlying_address
    and r.lending_pool_contract = t.version_pool qualify(ROW_NUMBER() over(PARTITION BY r._log_id
ORDER BY
    b.modified_timestamp DESC)) = 1
