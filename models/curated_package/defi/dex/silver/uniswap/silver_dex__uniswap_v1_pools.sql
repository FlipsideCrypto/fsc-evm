{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'pool_address',
    tags = ['silver_dex','defi','dex','curated']
) }}

WITH contract_mapping AS (
    {{ curated_contract_mapping(
        vars.CURATED_DEFI_DEX_SWAPS_CONTRACT_MAPPING
    ) }}
    WHERE
        version = 'v1'
),
pools AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        l.contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS token0, --All pools are token-ETH only, no token-token pairs
        '0x0000000000000000000000000000000000000000' AS token1, --native ETH, for pricing purposes
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS pool_address,
        m.protocol,
        m.version,
        m.type,
        CONCAT(
            m.protocol,
            '-',
            m.version
        ) AS platform,
        'NewExchange' AS event_name,
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
        topics [0] :: STRING = '0x9d42cb017eb05bd8944ab536a8b35bc68085931dd5f4356489801453923953f9' --NewExchange
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
    event_index,
    event_name,
    token0,
    token1,
    pool_address,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp
FROM
    pools qualify(ROW_NUMBER() over (PARTITION BY pool_address
ORDER BY
    modified_timestamp DESC)) = 1
