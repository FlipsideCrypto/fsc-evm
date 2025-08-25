{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    unique_key = "atoken_address",
    tags = ['silver','defi','lending','curated','aave','aave_tokens']
) }}

WITH aave_version_addresses AS (
    {{ curated_contract_mapping(
        vars.CURATED_DEFI_LENDING_CONTRACT_MAPPING
    ) }}
    WHERE
        type = 'aave_version_address'
),
DECODE AS (

    SELECT
        block_number AS atoken_created_block,
        contract_address AS a_token_address,
        tx_hash,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS underlying_asset,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS version_pool,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS treasury_address,
        utils.udf_hex_to_int(
            SUBSTR(
                segmented_data [2] :: STRING,
                27,
                40
            )
        ) :: INTEGER AS atoken_decimals,
        utils.udf_hex_to_string (
            segmented_data [7] :: STRING
        ) :: STRING AS atoken_name,
        utils.udf_hex_to_string (
            segmented_data [9] :: STRING
        ) :: STRING AS atoken_symbol,
        modified_timestamp,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] = '0xb19e051f8af41150ccccb3fc2c2d8d15f4a4cf434f32a559ba75fe73d6eea20b'
        

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
AND contract_address NOT IN (
    SELECT
        atoken_address
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
{% endif %}
),
a_token_step_1 AS (
    SELECT
        atoken_created_block,
        a_token_address,
        tx_hash,
        segmented_data,
        underlying_asset,
        version_pool,
        treasury_address,
        atoken_decimals,
        atoken_name,  
        atoken_symbol,
        modified_timestamp,
        _log_id
    FROM
        DECODE
    WHERE version_pool in (select distinct lower(contract_address) from aave_version_addresses)
    and underlying_asset not in (
        '0x0000000000000000000000000000000000000001',
        '0x0000000000000000000000000000000000000000')
),
debt_tokens AS (
    SELECT
        block_number AS atoken_created_block,
        contract_address AS a_token_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS underlying_asset,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS token_address,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 27, 40)) :: STRING AS token_stable_debt_address,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 27, 40)) :: STRING AS token_variable_debt_address,
        modified_timestamp,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] = '0x3a0ca721fc364424566385a1aa271ed508cc2c0949c2272575fb3013a163a45f'
        AND CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) IN (
            SELECT
                a_token_address
            FROM
                a_token_step_1
        )
)

SELECT
    A.tx_hash,
    A.atoken_created_block,
    A.version_pool,
    A.treasury_address,
    A.a_token_address AS atoken_address,
    b.token_stable_debt_address,
    b.token_variable_debt_address,
    t.protocol || '-' || t.version AS atoken_version,
    A.underlying_asset AS underlying_address,
    t.protocol,
    t.version,
    A.modified_timestamp,
    A._log_id
FROM
    a_token_step_1 A
    LEFT JOIN debt_tokens b
    ON A.a_token_address = b.token_address
    LEFT JOIN aave_version_addresses t
    ON A.version_pool = t.contract_address
    qualify(ROW_NUMBER() over(PARTITION BY A.underlying_asset,A.version_pool
ORDER BY
    A.atoken_created_block DESC)) = 1
