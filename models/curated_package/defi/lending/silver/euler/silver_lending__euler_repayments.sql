{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated','euler','repayments']
) }}

WITH token_meta AS (
    SELECT
        contract_address,
        token_name,
        token_symbol,
        token_decimals,
        segmented_data,
        creator,
        underlying_address,
        underlying_name,
        protocol,
        version,
        dToken
    FROM
        {{ ref('silver_lending__euler_tokens') }}
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
        contract_address AS market,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS borrower_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS repayer,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS repayed_amount,
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
        topics [0] :: STRING = '0x4cdde6e09bb755c9a5589ebaec640bbfedff1362d4b255ebf8339782b9942faa'
        and contract_address in (
            select
                distinct(contract_address)
            from
                token_meta
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
    r.contract_address,
    t.dToken AS protocol_market,
    t.underlying_address AS token_address,
    repayed_amount AS amount_unadj,
    repayer_address AS payer,
    borrower_address AS borrower,
    t.protocol || '-' || t.version AS platform,
    t.protocol,
    t.version,
    r._log_id,
    r.modified_timestamp,
    'Repay' AS event_name
FROM
    repay r
    LEFT JOIN token_meta t
    ON r.contract_address = t.contract_address qualify(ROW_NUMBER() over(PARTITION BY r._log_id
ORDER BY
    r.modified_timestamp DESC)) = 1
