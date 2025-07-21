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
        protocol = 'pancakeswap'
        AND version = 'v2'
        AND type IN ('ss_factory_1', 'ss_factory_2')
), 
pools AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        l.contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS pool_address,
        CASE
            WHEN m.type = 'ss_factory_1' THEN CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))
            WHEN m.type = 'ss_factory_2' THEN CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40))
        END AS tokenA,
        CASE
            WHEN m.type = 'ss_factory_1' THEN CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40))
            WHEN m.type = 'ss_factory_2' THEN CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40))
        END AS tokenB,
        CASE
            WHEN m.type = 'ss_factory_1' THEN NULL
            WHEN m.type = 'ss_factory_2' THEN CONCAT('0x', SUBSTR(segmented_data [2] :: STRING, 25, 40))
        END AS tokenC,
        CASE
            WHEN m.type = 'ss_factory_1' THEN NULL
            WHEN m.type = 'ss_factory_2' THEN CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40))
        END AS lp,
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
        {{ ref ('core__fact_event_logs') }}
        l 
        INNER JOIN contract_mapping m
        ON l.contract_address = m.contract_address
    WHERE
        topics [0] :: STRING IN (
            '0xa9551fb056fc743efe2a0a34e39f9769ad10166520df7843c09a66f82e148b97',
            '0x48dc7a1b156fe3e70ed5ed0afcb307661905edf536f15bb5786e327ea1933532'
        )
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
    event_index,
    event_name,
    contract_address,
    pool_address,
    tokenA,
    tokenB,
    tokenC,
    lp,
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
