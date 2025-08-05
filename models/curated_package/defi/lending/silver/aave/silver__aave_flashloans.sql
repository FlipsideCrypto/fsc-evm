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
flashloan AS (
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
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS target_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS initiator_address,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS market,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS flashloan_quantity,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: INTEGER AS premium_quantity,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: INTEGER AS refferalCode,
        COALESCE(
            origin_to_address,
            contract_address
        ) AS lending_pool_contract,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING IN (
            '0xefefaba5e921573100900a3ad9cf29f222d995fb3b6045797eaea7521bd8d6f0', --v3
            '0x631042c832b07452973831137f2d73e395028b44b250dedc5abb0ee766e168ac' --v2
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
    initiator_address AS initiator,
    target_address AS target,
    t.underlying_address AS token_address,
    t.underlying_symbol AS token_symbol,
    t.underlying_decimals AS token_decimals,
    flashloan_quantity AS flashloan_amount_unadj,
    flashloan_quantity / pow(
        10,
        t.underlying_decimals
    ) AS flashloan_amount,
    premium_quantity AS premium_amount_unadj,
    premium_quantity / pow(
        10,
        t.underlying_decimals
    ) AS premium_amount,
    t.protocol || '-' || t.version AS platform,
    t.protocol,
    t.version,
    f._log_id,
    f.modified_timestamp,
    'FlashLoan' AS event_name
FROM
    flashloan f
    INNER JOIN token_meta t
    ON f.market = t.underlying_address
    and f.lending_pool_contract = t.version_pool qualify(ROW_NUMBER() over(PARTITION BY f._log_id
ORDER BY
    f.modified_timestamp DESC)) = 1
