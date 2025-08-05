{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated','aave']
) }}

WITH atoken_meta AS (
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
withdraw AS(
    SELECT
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CASE 
            WHEN LOWER(CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40))) = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' 
                THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40))
        END AS market,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS useraddress,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS depositor,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS withdraw_amount,
        tx_hash,
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
            '0x3115d1449a7b732c986cba18244e897a450f61e1bb8d589cd2e69e6c8924f9f7',
            '0x9c4ed599cd8555b9c1e8cd7643240d7d71eb76b792948c49fcb4d411f7b6b3c6'
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
        atoken_meta
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
    depositor,
    t.atoken_address AS protocol_market,
    t.underlying_address AS token_address,
    t.underlying_symbol AS token_symbol,
    withdraw_amount AS amount_unadj,
    withdraw_amount / pow(
        10,
        t.underlying_decimals
    ) AS amount,
    lending_pool_contract,
    t.protocol || '-' || t.version AS platform,
    t.protocol,
    t.version,
    w._log_id,
    w.modified_timestamp,
    'Withdraw' AS event_name
FROM
    withdraw w
    LEFT JOIN atoken_meta t
    ON w.market = t.underlying_address qualify(ROW_NUMBER() over(PARTITION BY w._log_id
ORDER BY
    w.modified_timestamp DESC)) = 1
