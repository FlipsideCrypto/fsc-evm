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
Fluid Protocol - Borrows
Fluid uses a unified LogOperate event for all lending operations.
Borrows are identified when borrowAmount < 0 (negative value in two's complement).

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
        CASE
            WHEN CONCAT('0x', SUBSTR(l.topics[2]::STRING, 27, 40)) = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
            THEN '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}'
            ELSE CONCAT('0x', SUBSTR(l.topics[2]::STRING, 27, 40))
        END AS token_address,
        regexp_substr_all(SUBSTR(l.DATA, 3, len(l.DATA)), '.{64}') AS segmented_data,
        segmented_data[1]::STRING AS borrow_hex,
        CONCAT('0x', SUBSTR(segmented_data[3]::STRING, 25, 40)) AS borrow_to,
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

{# Filter for borrows only: negative borrowAmount (hex starts with 8-f in two's complement) #}
borrows_only AS (
    SELECT
        e.*,
        {# For negative int256 values, compute absolute value using two's complement.
           For practical DeFi amounts (< 2^120), we use the last 30 hex chars (120 bits).
           Formula: absolute_value = 2^120 - value_of_last_30_hex_chars
           2^120 fits in Snowflake's 38-digit numeric precision. #}
        POW(2, 120)::NUMBER(38,0) - TRY_TO_NUMBER(utils.udf_hex_to_int(RIGHT(borrow_hex, 30)), 38, 0) AS borrow_amount_abs
    FROM log_operate_events e
    WHERE
        {# Negative int256 values have first hex digit 8-f #}
        LEFT(borrow_hex, 1) IN ('8','9','a','b','c','d','e','f')
),

borrows_with_metadata AS (
    SELECT
        b.block_number,
        b.block_timestamp,
        b.tx_hash,
        b.event_index,
        b.origin_from_address,
        b.origin_to_address,
        b.origin_function_signature,
        b.contract_address,
        b.user_address AS borrower,
        b.borrow_to AS receiver,
        b.token_address,
        b.borrow_amount_abs AS amount_unadj,
        b.contract_address AS protocol_market,
        f.protocol,
        f.version,
        f.protocol || '-' || f.version AS platform,
        b._log_id,
        b.modified_timestamp
    FROM borrows_only b
    LEFT JOIN fluid_addresses f
        ON b.contract_address = f.contract_address
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
    borrower,
    receiver,
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
    'Borrow' AS event_name
FROM
    borrows_with_metadata
QUALIFY(ROW_NUMBER() OVER (PARTITION BY _log_id ORDER BY modified_timestamp DESC)) = 1
