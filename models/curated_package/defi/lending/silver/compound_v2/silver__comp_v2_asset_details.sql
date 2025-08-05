{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    unique_key = "token_address",
    tags = ['silver','defi','lending','curated','compound','compound_v2']
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
        topics [0] :: STRING = '0x7ac369dbd14fa5ea3f473ed67cc9d598964a77501540ba6751eb0b3decf5870d'
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
    ON c.contract_address = t.to_address
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
        ON l.contract_address = t.token_address qualify(ROW_NUMBER() over(PARTITION BY l.contract_address
    ORDER BY
        t.modified_timestamp ASC)) = 1
),
{% if is_incremental() %}
contract_detail_heal AS (
    SELECT
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        l.origin_from_address,
        l.token_address,
        c1.token_name,
        c1.token_symbol,
        c1.token_decimals,
        underlying_asset_address,
        c2.token_name AS underlying_name,
        c2.token_symbol AS underlying_symbol,
        c2.token_decimals AS underlying_decimals,
        l.protocol,
        l.version,
        l.modified_timestamp,
        l._log_id
    FROM
        {{ this }} l
        LEFT JOIN contracts c1
        ON c1.contract_address = l.underlying_asset_address
        LEFT JOIN contracts c2
        ON c2.contract_address = l.token_address
    WHERE
        (
            l.token_name IS NULL
            AND c1.token_name IS NOT NULL
        )
        OR (
            l.underlying_name IS NULL
            AND c2.token_name IS NOT NULL
        )
),
{% endif %}
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
    case when token_name = 'Compound Ether' then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' else underlying_asset_address end as underlying_asset_address,
    case when token_name = 'Compound Ether' then 'Ether' else underlying_name end as underlying_name,
    case when token_name = 'Compound Ether' then 'ETH' else underlying_symbol end as underlying_symbol,
    case when token_name = 'Compound Ether' then 18 else underlying_decimals end as underlying_decimals,
    protocol,
    version,
    modified_timestamp,
    _log_id
FROM
    final
{% if is_incremental() %}
UNION ALL
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    origin_from_address,
    token_address,
    token_name,
    token_symbol,
    token_decimals,
    underlying_asset_address,
    underlying_name,
    underlying_symbol,
    underlying_decimals,
    protocol,
    version,
    modified_timestamp,
    _log_id
FROM
    contract_detail_heal
{% endif %}