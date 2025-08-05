{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    unique_key = "atoken_address",
    tags = ['silver','defi','lending','curated','aave']
) }}

WITH treasury_addresses AS (
    {{ curated_contract_mapping(
        vars.CURATED_DEFI_LENDING_CONTRACT_MAPPING
    ) }}
    WHERE
        type = 'aave_treasury'
),
aave_version_addresses AS (
    {{ curated_contract_mapping(
        vars.CURATED_DEFI_LENDING_CONTRACT_MAPPING
    ) }}
    WHERE
        type = 'aave_version_address'
),
contracts AS (
    SELECT
        contract_address,
        token_name,
        token_decimals,
        token_symbol
    FROM
        {{ ref('silver__contracts') }}
),
{% if vars.GLOBAL_PROJECT_NAME == 'ethereum' %}

v1_v2_pool_addresses AS (
    {{ curated_contract_mapping(
        vars.CURATED_DEFI_LENDING_CONTRACT_MAPPING
    ) }}
    WHERE
        type = 'aave_pool_addresses'
),
aave_v1_v2_tokens AS (
    SELECT
        block_number AS atoken_created_block,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS a_token_address,
        null as atoken_stable_debt_address,
        null as atoken_variable_debt_address,
        contract_address AS pool_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS underlying_asset,
        modified_timestamp,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] = '0x1d9fcd0dc935b4778d5af97f55c4d7b2553257382f1ef25c412114c8eeebd88e'
    AND contract_address IN (
        SELECT
            contract_address
        FROM
            v1_v2_pool_addresses
        WHERE version = 'v1'
    )

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(
            modified_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) NOT IN (
    SELECT
        atoken_address
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
    UNION ALL
    SELECT
        block_number AS atoken_created_block,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS a_token_address,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 27, 40)) :: STRING AS atoken_stable_debt_address,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 27, 40)) :: STRING AS atoken_variable_debt_address,
        l.contract_address AS pool_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS underlying_address,
        l.modified_timestamp,
        CONCAT(
            l.tx_hash,
            '-',
            l.event_index
        ) AS _log_id
    FROM
        {{ ref('core__fact_event_logs') }} l
    WHERE
        topics [0] = '0x3a0ca721fc364424566385a1aa271ed508cc2c0949c2272575fb3013a163a45f'
{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(
            modified_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) NOT IN (
    SELECT
        atoken_address
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
aave_v1_v2_tokens_step_1 AS (
    SELECT
        atoken_created_block,
        a_token_address,
        segmented_data,
        CASE 
            WHEN v1.version = 'v1' THEN '0x398ec7346dcd622edc5ae82352f02be94c62d119'
            WHEN v1.version = 'v2' THEN '0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9'
            WHEN v1.version = 'v2.1' THEN '0x7937d4799803fbbe595ed57278bc4ca21f3bffcb'
        END AS version_pool,
        pool_address,
        underlying_asset,
        modified_timestamp,
        v1.version,
        v1.protocol,
        _log_id
    FROM
        aave_v1_v2_tokens t
    INNER JOIN v1_v2_pool_addresses v1
    ON v1.contract_address = t.pool_address
),
{% endif %}
DECODE AS (

    SELECT
        block_number AS atoken_created_block,
        contract_address AS a_token_address,
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
        MAX(
            modified_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND contract_address NOT IN (
    SELECT
        atoken_address
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
a_token_step_1 AS (
    SELECT
        atoken_created_block,
        a_token_address,
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
    WHERE treasury_address IN (
        SELECT
            contract_address
        FROM
            treasury_addresses
    )
    and version_pool in (select distinct contract_address from aave_version_addresses)
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
{% if vars.GLOBAL_PROJECT_NAME == 'ethereum' %}
SELECT
    A.atoken_created_block,
    A.version_pool,
    NULL AS treasury_address,
    C.token_symbol AS atoken_symbol,
    A.a_token_address AS atoken_address,
    NULL AS token_stable_debt_address,
    NULL AS token_variable_debt_address,
    C.token_decimals AS atoken_decimals,
    A.protocol || '-' || A.version AS atoken_version,
    C.token_name AS atoken_name,
    CASE
        WHEN A.underlying_asset = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 'ETH'
        ELSE c2.token_symbol
    END AS underlying_symbol,
    CASE
        WHEN A.underlying_asset = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ELSE A.underlying_asset
    END AS underlying_address,
    CASE
        WHEN A.underlying_asset = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 18
        ELSE c2.token_decimals
    END AS underlying_decimals,
    CASE
        WHEN A.underlying_asset = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 'Ethereum'
        ELSE c2.token_name
    END AS underlying_name,
    A.protocol,
    A.version,
    A.modified_timestamp,
    A._log_id
FROM
    aave_v1_v2_tokens_step_1 A
    LEFT JOIN contracts c
    ON c.contract_address = A.a_token_address
    LEFT JOIN contracts c2
    ON c2.contract_address = A.underlying_asset
UNION ALL
{% endif %}

SELECT
    A.atoken_created_block,
    A.version_pool,
    A.treasury_address,
    C.token_symbol AS atoken_symbol,
    A.a_token_address AS atoken_address,
    b.token_stable_debt_address,
    b.token_variable_debt_address,
    C.token_decimals AS atoken_decimals,
    t.protocol || '-' || t.version AS atoken_version,
    C.token_name AS atoken_name,
    c2.token_symbol AS underlying_symbol,
    A.underlying_asset AS underlying_address,
    c2.token_decimals AS underlying_decimals,
    c2.token_name AS underlying_name,
    t.protocol,
    t.version,
    A.modified_timestamp,
    A._log_id
FROM
    a_token_step_1 A
    LEFT JOIN debt_tokens b
    ON A.a_token_address = b.token_address
    LEFT JOIN contracts C
    ON C.contract_address = A.a_token_address
    LEFT JOIN contracts c2
    ON c2.contract_address = A.underlying_asset
    LEFT JOIN treasury_addresses t
    ON A.treasury_address = t.contract_address
    qualify(ROW_NUMBER() over(PARTITION BY A.a_token_address
ORDER BY
    A.atoken_created_block DESC)) = 1
