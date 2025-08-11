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
        underlying_symbol,
        underlying_decimals,
        'euler' as protocol,
        'v1' as version,
        dToken
    FROM
        {{ ref('silver__euler_tokens') }}
),
base AS(
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
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 42)) AS account,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS repay_quantity,
        origin_from_address AS borrower,
        modified_timestamp,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING = '0x5c16de4f8b59bd9caf0f49a545f25819a895ed223294290b408242e72a594231'
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
    d.contract_address,
    t.dToken AS protocol_market,
    t.underlying_address AS token_address,
    t.underlying_symbol AS token_symbol,
    repay_quantity AS amount_unadj,
    repay_quantity / pow(
        10,
        t.underlying_decimals
    ) AS amount,
    borrower,
    t.protocol || '-' || t.version AS platform,
    t.protocol,
    t.version,
    d._log_id,
    d.modified_timestamp,
    'Repay' AS event_name
FROM
    base d
    INNER JOIN token_meta t
    ON d.contract_address = t.contract_address qualify(ROW_NUMBER() over(PARTITION BY d._log_id
ORDER BY
    d.modified_timestamp DESC)) = 1