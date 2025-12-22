{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated','fluid']
) }}

{#
Fluid Protocol - Repayments
Fluid uses a unified LogOperate event for all lending operations.
Repayments are identified when borrowAmount > 0 (positive value).

LogOperate Event Signature: 0x4d93b232a24e82b284ced7461bf4deacffe66759d5c24513e6f29e571ad78d15
- topics[1]: user address
- topics[2]: token address
- data[0]: supplyAmount (int256) - positive = deposit, negative = withdraw
- data[1]: borrowAmount (int256) - positive = repay, negative = borrow
- data[2]: withdrawTo address
- data[3]: borrowTo address
#}

WITH fluid_addresses AS (
    {{ curated_contract_mapping(
        vars.CURATED_DEFI_LENDING_CONTRACT_MAPPING
    ) }}
    WHERE
        type = 'fluid_liquidity_layer'
),

log_operate_events AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        l.event_index,
        l.origin_from_address,
        l.origin_to_address,
        l.origin_function_signature,
        l.contract_address,
        CONCAT('0x', SUBSTR(l.topics[1]::STRING, 27, 40)) AS user_address,
        CONCAT('0x', SUBSTR(l.topics[2]::STRING, 27, 40)) AS token_address,
        regexp_substr_all(SUBSTR(l.DATA, 3, len(l.DATA)), '.{64}') AS segmented_data,
        segmented_data[1]::STRING AS borrow_hex,
        l.modified_timestamp,
        CONCAT(l.tx_hash::STRING, '-', l.event_index::STRING) AS _log_id
    FROM
        {{ ref('core__fact_event_logs') }} l
    WHERE
        l.contract_address IN (SELECT contract_address FROM fluid_addresses)
        AND l.topics[0]::STRING = '0x4d93b232a24e82b284ced7461bf4deacffe66759d5c24513e6f29e571ad78d15'
        AND l.tx_succeeded
        AND l.block_timestamp >= '{{ vars.CURATED_START_TIMESTAMP }}'

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

{# Filter for repayments only: positive borrowAmount (hex doesn't start with 8-f) #}
repayments_only AS (
    SELECT
        e.*,
        TRY_TO_NUMBER(utils.udf_hex_to_int(borrow_hex)) AS borrow_amount_raw
    FROM log_operate_events e
    WHERE
        {# Positive int256 values have first hex digit 0-7 #}
        LEFT(borrow_hex, 1) NOT IN ('8','9','a','b','c','d','e','f')
        AND borrow_hex != '0000000000000000000000000000000000000000000000000000000000000000'
),

repayments_with_metadata AS (
    SELECT
        r.block_number,
        r.block_timestamp,
        r.tx_hash,
        r.event_index,
        r.origin_from_address,
        r.origin_to_address,
        r.origin_function_signature,
        r.contract_address,
        r.user_address AS payer,
        r.user_address AS borrower,
        r.token_address,
        r.borrow_amount_raw AS amount_unadj,
        r.contract_address AS protocol_market,
        f.protocol,
        f.version,
        f.protocol || '-' || f.version AS platform,
        r._log_id,
        r.modified_timestamp
    FROM repayments_only r
    LEFT JOIN fluid_addresses f
        ON r.contract_address = f.contract_address
)

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    payer,
    borrower,
    protocol_market,
    token_address,
    amount_unadj,
    platform,
    protocol,
    version,
    modified_timestamp AS _inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    _log_id,
    'Repay' AS event_name
FROM
    repayments_with_metadata
QUALIFY(ROW_NUMBER() OVER (PARTITION BY _log_id ORDER BY modified_timestamp DESC)) = 1
