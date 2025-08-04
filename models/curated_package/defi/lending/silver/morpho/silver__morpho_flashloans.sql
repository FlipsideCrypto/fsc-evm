{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated']
) }}

{# Get variables #}
{% set vars = return_vars() %}

WITH morpho_blue_addresses AS (
    {{ curated_contract_mapping(
        vars.CURATED_DEFI_LENDING_CONTRACT_MAPPING
    ) }}
    WHERE
        type = 'morpho_blue_address'
),

flashloan AS(

    SELECT
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        l.event_index,
        l.origin_from_address,
        l.origin_to_address,
        l.origin_function_signature,
        l.contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS caller,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS token,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS flashloan_quantity,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
    WHERE
        topics [0] :: STRING = '0xc76f1b4fe4396ac07a9fa55a415d4ca430e72651d37d3401f3bed7cb13fc4f12'
        AND contract_address IN (
            SELECT
                contract_address
            FROM
                morpho_blue_addresses
        )

{% if is_incremental() %}
AND l.modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND l.modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    f.contract_address,
    caller AS initiator,
    null as target,
    token AS protocol_market,
    C.token_symbol,
    flashloan_quantity AS flashloan_amount_unadj,
    flashloan_quantity / pow(
        10,
        C.token_decimals
    ) AS flashloan_amount,
    null as premium_amount_unadj,
    null as premium_amount,
    m.protocol || '-' || m.version AS platform,
    m.protocol,
    m.version,
    f._log_id,
    f.modified_timestamp,
    'Flashloan' AS event_name
FROM
    flashloan f
    LEFT JOIN {{ ref('silver__contracts') }} C
    ON f.token = C.contract_address
    LEFT JOIN morpho_blue_addresses m
    ON m.contract_address = f.contract_address
