{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated','aave','interest_rates']
) }}

WITH --borrows from Aave LendingPool contracts
token_meta AS (

    SELECT
        atoken_created_block,
        version_pool,
        treasury_address,
        atoken_address,
        token_stable_debt_address,
        token_variable_debt_address,
        atoken_version,
        underlying_address,
        protocol,
        version,
        modified_timestamp,
        _log_id
    FROM
        {{ ref('silver_lending__aave_tokens') }}
    UNION ALL
    SELECT
        atoken_created_block,
        version_pool,
        NULL AS  treasury_address,
        atoken_address,
        NULL AS token_stable_debt_address,
        NULL AS token_variable_debt_address,
        atoken_version,
        underlying_address,
        protocol,
        version,
        modified_timestamp,
        _log_id
    FROM
        {{ ref('silver_lending__aave_ethereum_tokens') }}
    
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
        contract_address as lending_pool_contract,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS token_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS liquidity_rate,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS stable_borrow_rate,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            )
        ) AS variable_borrow_rate,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [3] :: STRING
            )
        ) AS liquidity_index,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [4] :: STRING
            )
        ) AS variable_borrow_index,
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
    liquidity_rate AS supply_rate_unadj,
    stable_borrow_rate AS stable_borrow_rate_unadj,
    variable_borrow_rate AS variable_borrow_rate_unadj,
    r.liquidity_index,
    r.variable_borrow_index,
    r.lending_pool_contract,
    t.protocol || '-' || t.version AS platform,
    t.protocol,
    t.version,
    r._log_id,
    r.modified_timestamp,
    'Borrow' AS event_name
FROM
    reserve_data r
    LEFT JOIN token_meta t
    ON r.token_address = t.underlying_address
    and r.lending_pool_contract = t.version_pool qualify(ROW_NUMBER() over(PARTITION BY r._log_id
ORDER BY
    r.modified_timestamp DESC)) = 1
