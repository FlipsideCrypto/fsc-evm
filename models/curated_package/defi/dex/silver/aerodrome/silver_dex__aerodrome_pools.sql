{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "pool_address",
    tags = ['silver_dex','defi','dex','curated']
) }}

WITH contract_mapping AS (
    {{ curated_contract_mapping(
        vars.CURATED_DEFI_DEX_SWAPS_CONTRACT_MAPPING
    ) }}
    WHERE
        protocol = 'aerodrome'
        AND version = 'v1'
        AND type = 'factory'
),
created_pools AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        l.contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        LOWER(CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40))) AS token0,
        LOWER(CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))) AS token1,
        LOWER(CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40))) AS stable,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [1] :: STRING
        ) :: INTEGER AS pool_number,
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
        topics [0] = '0x2128d88d14c80cb081c1252a5acff7a264671bf199ce226b53788fb26065005e'
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
    contract_address,
    event_name,
    token0,
    token1,
    CASE
        WHEN stable = '0x0000000000000000000000000000000000000001' THEN TRUE
        WHEN stable = '0x0000000000000000000000000000000000000000' THEN FALSE
    END AS stable,
    pool_number,
    pool_address,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp
FROM
    created_pools qualify(ROW_NUMBER() over (PARTITION BY pool_address
ORDER BY
    modified_timestamp DESC)) = 1
