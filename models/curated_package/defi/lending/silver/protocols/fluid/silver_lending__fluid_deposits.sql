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
Fluid Protocol - Deposits
Fluid uses a unified LogOperate event for all lending operations.
Deposits are identified when supplyAmount > 0 (positive value).

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
        segmented_data[0]::STRING AS supply_hex,
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

{# Filter for deposits only: positive supplyAmount (hex doesn't start with 8-f) #}
deposits_only AS (
    SELECT
        e.*,
        TRY_TO_NUMBER(utils.udf_hex_to_int(supply_hex)) AS supply_amount_raw
    FROM log_operate_events e
    WHERE
        {# Positive int256 values have first hex digit 0-7 #}
        LEFT(supply_hex, 1) NOT IN ('8','9','a','b','c','d','e','f')
        AND supply_hex != '0000000000000000000000000000000000000000000000000000000000000000'
),

deposits_with_metadata AS (
    SELECT
        d.block_number,
        d.block_timestamp,
        d.tx_hash,
        d.event_index,
        d.origin_from_address,
        d.origin_to_address,
        d.origin_function_signature,
        d.contract_address,
        d.user_address AS depositor,
        d.token_address,
        d.supply_amount_raw AS amount_unadj,
        d.contract_address AS protocol_market,
        f.protocol,
        f.version,
        f.protocol || '-' || f.version AS platform,
        d._log_id,
        d.modified_timestamp
    FROM deposits_only d
    LEFT JOIN fluid_addresses f
        ON d.contract_address = f.contract_address
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
    depositor,
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
    'Deposit' AS event_name
FROM
    deposits_with_metadata
QUALIFY(ROW_NUMBER() OVER (PARTITION BY _log_id ORDER BY modified_timestamp DESC)) = 1
