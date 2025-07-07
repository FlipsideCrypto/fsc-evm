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
        vars.CURATED_DEX_POOLS_CONTRACT_MAPPING
    ) }}
    WHERE
        protocol = 'dodo'
        AND version IN ('v2')
),
pools AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS baseToken,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS quoteToken,
        CONCAT('0x', SUBSTR(segmented_data [2] :: STRING, 25, 40)) AS creator,
        CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40)) AS pool_address,
        m.protocol,
        m.version,
        CONCAT(
            m.protocol,
            '-',
            m.version
        ) AS platform,
        CASE
            WHEN topics [0] :: STRING = '0x8494fe594cd5087021d4b11758a2bbc7be28a430e94f2b268d668e5991ed3b8a' THEN 'NewDPP'
            WHEN topics [0] :: STRING = '0xbc1083a2c1c5ef31e13fb436953d22b47880cf7db279c2c5666b16083afd6b9d' THEN 'NewDSP'
            WHEN topics [0] :: STRING = '0xaf5c5f12a80fc937520df6fcaed66262a4cc775e0f3fceaf7a7cfe476d9a751d' THEN 'NewDVM'
        END AS event_name,
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
        topics [0] :: STRING IN (
            '0x8494fe594cd5087021d4b11758a2bbc7be28a430e94f2b268d668e5991ed3b8a',
            --NewDPP
            '0xbc1083a2c1c5ef31e13fb436953d22b47880cf7db279c2c5666b16083afd6b9d',
            --NewDSP
            '0xaf5c5f12a80fc937520df6fcaed66262a4cc775e0f3fceaf7a7cfe476d9a751d' --NewDVM
        )
        AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    contract_address,
    baseToken AS base_token,
    quoteToken AS quote_token,
    creator,
    pool_address,
    platform,
    protocol,
    version,
    _log_id,
    modified_timestamp
FROM
    pools qualify(ROW_NUMBER() over (PARTITION BY pool_address
ORDER BY
    modified_timestamp DESC)) = 1
