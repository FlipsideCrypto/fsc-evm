{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    tags = ['silver','defi','lending','curated']
) }}

{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Get fraxlend origin from address #}
WITH fraxlend_origin_from_address AS (
    {{ curated_contract_mapping(
        vars.CURATED_DEFI_LENDING_CONTRACT_MAPPING
    ) }}
    WHERE
        type = 'fraxlend_origin_from_address'
),

logs AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        contract_address,
        topics,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CASE
            WHEN topics [0] = '0xb7f7e57b7bb3a5186ad1bd43405339ba361555344aec7a4be01968e88ee3883e' THEN CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40))
            WHEN topics [0] = '0x9303649990c462969a3c46d4e2c758166e92f5a4b18c67f26d3e58d2b0660e67' THEN CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 42))
            WHEN topics [0] = '0xc6fa598658c9cdf9eaa5f76414ef17a38a7f74c0e719a0571a3f73d9ecd755b7' THEN CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 42))
        END AS pool_address,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] IN (
            '0xb7f7e57b7bb3a5186ad1bd43405339ba361555344aec7a4be01968e88ee3883e',
            '0x9303649990c462969a3c46d4e2c758166e92f5a4b18c67f26d3e58d2b0660e67',
            '0xc6fa598658c9cdf9eaa5f76414ef17a38a7f74c0e719a0571a3f73d9ecd755b7'
        )
        and origin_from_address in (
            select contract_address from fraxlend_origin_from_address
        )

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '36 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
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
),
logs_transform AS (
    SELECT
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        l.event_index,
        l.contract_address,
        l.origin_from_address,
        l.origin_to_address,
        pool_address AS frax_market_address,
        NAME AS frax_market_name,
        symbol AS frax_market_symbol,
        decimals,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 42)) AS underlying_asset,
        CONCAT(
            l.tx_hash,
            '-',
            l.event_index
        ) AS _log_id,
        l.modified_timestamp AS _inserted_timestamp
    FROM
        logs l
        LEFT JOIN contracts c
        ON c.contract_address = pool_address
)
SELECT
    l.tx_hash,
    l.block_number,
    l.block_timestamp,
    l.event_index,
    l.contract_address,
    l.origin_from_address,
    l.origin_to_address,
    l.frax_market_address,
    l.frax_market_name,
    l.frax_market_symbol,
    l.decimals as frax_market_decimals,
    c.token_name AS underlying_name,
    l.underlying_asset,
    c.token_symbol AS underlying_symbol,
    c.token_decimals AS underlying_decimals,
    f.protocol || '-' || f.version AS platform,
    f.protocol,
    f.version,
    l._log_id,
    l._inserted_timestamp,
FROM
    logs_transform l
LEFT JOIN 
    contracts c
ON
    c.contract_address =  underlying_asset
LEFT JOIN 
    fraxlend_origin_from_address f
ON
    f.contract_address = l.origin_from_address
WHERE
    frax_market_name IS NOT NULL
AND 
    c.token_decimals IS NOT NULL