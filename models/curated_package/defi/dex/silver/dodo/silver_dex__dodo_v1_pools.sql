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
        vars.CURATED_DEFI_DEX_POOLS_CONTRACT_MAPPING
    ) }}
    WHERE
        protocol = 'dodo'
        AND version IN ('v1')
),
pools AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        l.contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS newBorn,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS baseToken,
        CONCAT('0x', SUBSTR(segmented_data [2] :: STRING, 25, 40)) AS quoteToken,
        m.protocol,
        m.version,
        CONCAT(
            m.protocol,
            '-',
            m.version
        ) AS platform,
        'DODOBirth' AS event_name,
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
        topics [0] :: STRING = '0x5c428a2e12ecaa744a080b25b4cda8b86359c82d726575d7d747e07708071f93' --DODOBirth
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
    newBorn AS pool_address,
    baseToken AS base_token,
    quoteToken AS quote_token,
    platform,
    protocol,
    version,
    _log_id,
    modified_timestamp
FROM
    pools qualify(ROW_NUMBER() over (PARTITION BY pool_address
ORDER BY
    modified_timestamp DESC)) = 1
