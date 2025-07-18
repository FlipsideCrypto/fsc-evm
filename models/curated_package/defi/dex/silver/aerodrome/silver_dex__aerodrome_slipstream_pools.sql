{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "pool_address",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_dex','defi','dex','curated']
) }}

WITH contract_mapping AS (
    {{ curated_contract_mapping(
        vars.CURATED_DEFI_DEX_SWAPS_CONTRACT_MAPPING
    ) }}
    WHERE
        protocol = 'aerodrome'
        AND version = 'v1'
        AND type = 'slipstream'
),
created_pools AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        l.contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        LOWER(CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40))) AS token0_address,
        LOWER(CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))) AS token1_address,
        utils.udf_hex_to_int(
            's2c',
            topics [3] :: STRING
        ) :: INTEGER AS tick_spacing,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS pool_address,
        m.protocol,
        m.version,
        m.type,
        CONCAT(m.protocol, '-', m.version) AS platform,
        'PoolCreated' AS event_name,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        INNER JOIN contract_mapping m
        ON l.contract_address = m.contract_address
    WHERE
        topics [0] = '0xab0d57f0df537bb25e80245ef7748fa62353808c54d6e528a9dd20887aed9ac2'
        AND tx_succeeded

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
initial_info AS (
    SELECT
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        utils.udf_hex_to_int('s2c', CONCAT('0x', segmented_data [0] :: STRING)) :: FLOAT AS init_sqrtPriceX96,
        utils.udf_hex_to_int('s2c', CONCAT('0x', segmented_data [1] :: STRING)) :: FLOAT AS init_tick,
        pow(
            1.0001,
            init_tick
        ) AS init_price_1_0_unadj,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING = '0x98636036cb66a9c19a37435efc1e90142190214e8abeb821bdba3f2990dd4c95'
        AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    p.contract_address,
    p.event_name,
    token0_address,
    token1_address,
    tick_spacing,
    pool_address,
    COALESCE(
        init_tick,
        0
    ) AS init_tick,
    p.platform,
    p.protocol,
    p.version,
    p.type,
    p._log_id,
    p.modified_timestamp
FROM
    created_pools p
    LEFT JOIN initial_info i
    ON p.pool_address = i.contract_address
qualify(ROW_NUMBER() over(PARTITION BY pool_address
ORDER BY
    _inserted_timestamp DESC)) = 1
