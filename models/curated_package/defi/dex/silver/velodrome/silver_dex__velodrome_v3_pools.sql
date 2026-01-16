{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'pool_address',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_dex','defi','dex','curated']
) }}

WITH contract_mapping AS (
    {{ curated_contract_mapping(
        vars.CURATED_DEFI_DEX_SWAPS_CONTRACT_MAPPING
    ) }}
    WHERE
        protocol = 'velodrome'
        AND version = 'v3'
),
pools_v1 AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        l.contract_address AS factory_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        LOWER(CONCAT('0x', SUBSTR(topics[1]::STRING, 27, 40))) AS token0,
        LOWER(CONCAT('0x', SUBSTR(topics[2]::STRING, 27, 40))) AS token1,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                segmented_data[0]::STRING
            )
        ) AS tick_spacing,
        LOWER(CONCAT('0x', SUBSTR(segmented_data[1]::STRING, 25, 40))) AS pool_address,
        'velodrome' AS protocol,
        'v3' AS version,
        'cl_factory' AS type,
        'velodrome-v3' AS platform,
        'PoolCreated' AS event_name,
        CONCAT(tx_hash::STRING, '-', event_index::STRING) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }} l
        INNER JOIN contract_mapping m
        ON l.contract_address = m.contract_address
    WHERE
        topics[0]::STRING = '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118' -- PoolCreated (Uniswap v3 style)
        AND m.type = 'cl_factory_v1'
        AND tx_succeeded

{% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
{% endif %}
),
pools_v2 AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        l.contract_address AS factory_address,
        LOWER(CONCAT('0x', SUBSTR(topics[1]::STRING, 27, 40))) AS token0,
        LOWER(CONCAT('0x', SUBSTR(topics[2]::STRING, 27, 40))) AS token1,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                topics[3]::STRING
            )
        ) AS tick_spacing,
        LOWER(CONCAT('0x', SUBSTR(data, 27, 40))) AS pool_address,
        'velodrome' AS protocol,
        'v3' AS version,
        'cl_factory' AS type,
        'velodrome-v3' AS platform,
        'PoolCreated' AS event_name,
        CONCAT(tx_hash::STRING, '-', event_index::STRING) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }} l
        INNER JOIN contract_mapping m
        ON l.contract_address = m.contract_address
    WHERE
        topics[0]::STRING = '0xab0d57f0df537bb25e80245ef7748fa62353808c54d6e528a9dd20887aed9ac2' -- PoolCreated (Velodrome Superchain style)
        AND m.type = 'cl_factory_v2'
        AND tx_succeeded

{% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
{% endif %}
),
all_pools AS (
    SELECT * FROM pools_v1
    UNION ALL
    SELECT * FROM pools_v2
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    factory_address,
    event_name,
    token0,
    token1,
    tick_spacing,
    pool_address,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp
FROM
    all_pools
QUALIFY ROW_NUMBER() OVER (PARTITION BY pool_address ORDER BY modified_timestamp DESC) = 1
