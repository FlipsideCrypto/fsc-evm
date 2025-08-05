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
repay AS(
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
        CASE 
            WHEN LOWER(CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40))) = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' 
                THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40))
        END AS market,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS borrower_address,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS repayer,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS repayed_amount,
        COALESCE(
            origin_to_address,
            contract_address
        ) AS lending_pool_contract,
        origin_from_address AS repayer_address,
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
            '0x4cdde6e09bb755c9a5589ebaec640bbfedff1362d4b255ebf8339782b9942faa',
            '0xa534c8dbe71f871f9f3530e97a74601fea17b426cae02e1c5aee42c96c784051'
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
    t.underlying_address AS token_address,
    t.underlying_symbol AS token_symbol,
    repayed_amount AS amount_unadj,
    repayed_amount / pow(
        10,
        t.underlying_decimals
    ) AS amount,
    repayer_address AS payer,
    borrower_address AS borrower,
    lending_pool_contract,
    t.protocol || '-' || t.version AS platform,
    t.protocol,
    t.version,
    r._log_id,
    r.modified_timestamp,
    'Repay' AS event_name
FROM
    repay r
    LEFT JOIN token_meta t
    ON r.market = t.underlying_address
    and r.lending_pool_contract = t.version_pool qualify(ROW_NUMBER() over(PARTITION BY r._log_id
ORDER BY
    r.modified_timestamp DESC)) = 1
