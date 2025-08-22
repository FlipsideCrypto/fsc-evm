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

WITH token_meta AS (

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
),
deposits AS(
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
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 42)) AS userAddress,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS deposit_quantity,
        origin_from_address AS depositor,
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
            '0xde6857219544bb5b7746f48ed30be6386fefc61b2f864cacf559893bf50fd951',
            '0x2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61'
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
AND contract_address IN (
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
    t.atoken_address AS protocol_market,
    market AS token_address,
    deposit_quantity AS amount_unadj,
    depositor,
    lending_pool_contract,
    t.protocol || '-' || t.version AS platform,
    t.protocol,
    t.version,
    d._log_id,
    d.modified_timestamp,
    'Deposit' AS event_name
FROM
    deposits d
    LEFT JOIN token_meta t
    ON d.market = t.underlying_address
    and d.lending_pool_contract = t.version_pool qualify(ROW_NUMBER() over(PARTITION BY d._log_id
ORDER BY
    d.modified_timestamp DESC)) = 1
