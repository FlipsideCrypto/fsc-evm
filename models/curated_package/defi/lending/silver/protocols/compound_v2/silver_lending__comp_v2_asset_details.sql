
  -- depends_on: {{ ref('silver_lending__token_metadata') }}
{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    unique_key = "token_address",
    merge_exclude_columns = ['inserted_timestamp'],
    tags = ['silver','defi','lending','curated','comp_v2']
) }}

WITH origin_from_addresses AS (
    {{ curated_contract_mapping(
        vars.CURATED_DEFI_LENDING_CONTRACT_MAPPING
    ) }}
    WHERE
        type = 'comp_v2_origin_from_address'
),
contracts AS (
    SELECT
        contract_address,
        token_name,
        token_symbol,
        token_decimals
    FROM
        {{ ref('silver__contracts') }}
),
log_pull AS (
    SELECT
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        l.origin_from_address,
        l.contract_address,
        C.token_name,
        C.token_symbol,
        C.token_decimals,
        l.modified_timestamp,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        LEFT JOIN contracts C
        ON C.contract_address = l.contract_address
    WHERE
        topics [0] :: STRING in(
        '0x7ac369dbd14fa5ea3f473ed67cc9d598964a77501540ba6751eb0b3decf5870d',
        '0x70aea8d848e8a90fb7661b227dc522eb6395c3dac71b63cb59edd5c9899b2364',
        '0x17d6db5ffe5a3d1c3d7a98194dca4f7d808d621e6ff9077ed574513d553a2a85'--joelend topic
        )
        AND origin_from_address IN (
            SELECT
                contract_address
            FROM
                origin_from_addresses
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
),
traces_pull AS (
    SELECT
        block_number,
        tx_hash,
        type,
        block_timestamp,
        from_address AS token_address,
        LEFT(input, 10) AS function_sig,
        regexp_substr_all(SUBSTR(input, 11), '.{64}') AS segmented_input,
        CONCAT('0x', SUBSTR(segmented_input[0]::STRING, 25)) AS underlying_asset_address,
        utils.udf_hex_to_string(
        segmented_input[array_size(segmented_input) - 3]::STRING) AS token_name,
        utils.udf_hex_to_string(
        segmented_input[array_size(segmented_input) - 1]::STRING) AS token_symbol,
        TRY_TO_NUMBER(
        utils.udf_hex_to_int(
        segmented_input[6]::STRING)) AS token_decimals,
        modified_timestamp
    FROM
        {{ ref('core__fact_traces') }}
        t
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                log_pull
        )
        AND TYPE = 'DELEGATECALL'
        AND trace_index = 1
    UNION
        SELECT
        block_number,
        tx_hash,
        type,
        block_timestamp,
        from_address AS token_address,
        LEFT(input, 10) AS function_sig,
        regexp_substr_all(SUBSTR(input, 11), '.{64}') AS segmented_input,
        to_address AS underlying_asset_address,
        c.token_name,
        c.token_symbol,
        c.token_decimals,
        modified_timestamp
    FROM
        {{ ref('core__fact_traces') }}
        t
    LEFT JOIN contracts c
    ON c.contract_address = t.from_address
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                log_pull
        )
        and function_sig = '0x18160ddd'
        and from_address in (select contract_address from log_pull)
),
underlying_details AS (
    SELECT
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        l.origin_from_address,
        l.contract_address,
        COALESCE(t.token_name, l.token_name) AS token_name,
        COALESCE(t.token_symbol, l.token_symbol) AS token_symbol,
        COALESCE(t.token_decimals, l.token_decimals) AS token_decimals,
        t.underlying_asset_address,
        l.modified_timestamp,
        l._log_id
    FROM
        log_pull l
        LEFT JOIN traces_pull t
        ON l.contract_address = t.token_address
        AND l.tx_hash = t.tx_hash 
),
final AS (
    SELECT  
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        l.origin_from_address,
        l.contract_address AS token_address,
        l.token_name,
        l.token_symbol,
        l.token_decimals,
        l.underlying_asset_address,
        C.token_name AS underlying_name,
        C.token_symbol AS underlying_symbol,
        C.token_decimals AS underlying_decimals,
        o.protocol,
        o.version,
        l.modified_timestamp,
        l._log_id
    FROM
        underlying_details l
        LEFT JOIN contracts C
        ON C.contract_address = l.underlying_asset_address
        LEFT JOIN origin_from_addresses o
        ON o.contract_address = l.origin_from_address
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    origin_from_address,
    token_address,
    token_name,
    token_symbol,
    token_decimals,
    CASE 
        WHEN token_symbol LIKE '%' || '{{ vars.GLOBAL_NATIVE_ASSET_SYMBOL }}' || '%' AND underlying_symbol IS NULL
            THEN '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}'
        ELSE underlying_asset_address 
    END AS underlying_asset_address,
    CASE 
        WHEN token_symbol LIKE '%' || '{{ vars.GLOBAL_NATIVE_ASSET_SYMBOL }}' || '%' AND underlying_name IS NULL 
            THEN '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_SYMBOL }}'
        ELSE underlying_name 
    END AS underlying_name,
    CASE 
        WHEN token_symbol LIKE '%' || '{{ vars.GLOBAL_NATIVE_ASSET_SYMBOL }}' || '%' AND underlying_symbol IS NULL 
            THEN '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_SYMBOL }}'
        ELSE underlying_symbol 
    END AS underlying_symbol,
    CASE 
        WHEN token_symbol LIKE '%' || '{{ vars.GLOBAL_NATIVE_ASSET_SYMBOL }}' || '%' AND underlying_decimals IS NULL 
            THEN 18
        ELSE underlying_decimals 
    END AS underlying_decimals,
    protocol,
    version,
    modified_timestamp as _inserted_timestamp,
    SYSDATE() as modified_timestamp,
    SYSDATE() as inserted_timestamp,
    _log_id
FROM
    final
        qualify(ROW_NUMBER() over(PARTITION BY token_address
    ORDER BY
        CASE WHEN underlying_symbol IS NOT NULL THEN 0 ELSE 1 END ASC,block_number ASC)) = 1