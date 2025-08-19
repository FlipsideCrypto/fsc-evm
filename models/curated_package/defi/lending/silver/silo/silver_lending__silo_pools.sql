{{ config(
    materialized = 'incremental',
    unique_key = "silo_address",
    tags = ['silver','defi','lending','curated','silo']
) }}

{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH silo_factory_addresses AS (
    {{ curated_contract_mapping(
        vars.CURATED_DEFI_LENDING_CONTRACT_MAPPING
    ) }}
    WHERE
        type in('silo_factory', 'silo_tokens_factory')
),
 logs_pull AS (

    SELECT
        block_number,
        tx_hash,
        contract_address,
        event_index,
        data,
        topics,
        modified_timestamp,
        CASE
            WHEN contract_address in (select contract_address from silo_factory_addresses where type = 'silo_factory') THEN CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))
            WHEN contract_address in (select contract_address from silo_factory_addresses where type = 'silo_tokens_factory') THEN CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40))
            ELSE NULL
        END AS tokens
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address IN (
            SELECT
                contract_address
            FROM
                silo_factory_addresses
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
contracts AS (
    SELECT
        contract_address,
        token_name,
        token_decimals,
        token_symbol
    FROM
        {{ ref('silver__contracts') }}
    WHERE
        contract_address IN (
            SELECT
                tokens
            FROM
                logs_pull
        )
),
silo_pull AS (
    SELECT
        block_number AS silo_create_block,
        tx_hash,
        l.contract_address AS factory_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS silo_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS token_address,
        utils.udf_hex_to_int(
            SUBSTR(
                segmented_data [0] :: STRING,
                27,
                40
            )
        ) :: INTEGER AS version,
        modified_timestamp,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id
    FROM
        logs_pull l
    WHERE
        l.contract_address in (select contract_address from silo_factory_addresses where type = 'silo_factory')
),
silo_collateral_token AS (
    SELECT
        tx_hash,
        CONCAT('0x', SUBSTR(topics [0] :: STRING, 27, 40)) AS topic_0,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS protocol_collateral_token_address,
        C.token_symbol,
        C.token_decimals
    FROM
        logs_pull l
        LEFT JOIN contracts C
        ON CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) = C.contract_address
    WHERE
        l.contract_address in (select contract_address from silo_factory_addresses where type = 'silo_token_factory')
        AND topics [0] :: STRING = '0xd97e9f840332422474cda9bb0976c87735b44cda62a3fe2a4e13e2e862671812'
),
silo_debt_token AS (
    SELECT
        tx_hash,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS protocol_debt_token_address,
        C.token_symbol,
        C.token_decimals
    FROM
        logs_pull l
        LEFT JOIN contracts C
        ON CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) = C.contract_address
    WHERE
        l.contract_address in (select contract_address from silo_factory_addresses where type = 'silo_token_factory')
        AND topics [0] :: STRING = '0x94f128ebf0749edb8bb9d165d016ce008a16bc82cbd40cc81ded2be79140d020'
)
SELECT
    silo_create_block,
    l.tx_hash AS creation_hash,
    factory_address,
    silo_address,
    l.token_address,
    C.token_name,
    C.token_symbol,
    C.token_decimals,
    ct.protocol_collateral_token_address,
    ct.token_symbol AS protocol_collateral_token_symbol,
    ct.token_decimals AS protocol_collateral_token_decimals,
    dt.protocol_debt_token_address,
    dt.token_symbol AS protocol_debt_token_symbol,
    dt.token_decimals AS protocol_debt_token_decimals,
    sf.protocol,
    sf.version,
    sf.protocol || '-' || sf.version AS platform,
    l._log_id,
    l.modified_timestamp
FROM
    silo_pull l
    LEFT JOIN contracts C
    ON C.contract_address = l.token_address
    LEFT JOIN silo_collateral_token ct
    ON ct.tx_hash = l.tx_hash
    LEFT JOIN silo_debt_token dt
    ON dt.tx_hash = l.tx_hash
    LEFT JOIN silo_factory_addresses sf
    ON sf.contract_address = l.factory_address
WHERE
    silo_address IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    l.modified_timestamp DESC)) = 1