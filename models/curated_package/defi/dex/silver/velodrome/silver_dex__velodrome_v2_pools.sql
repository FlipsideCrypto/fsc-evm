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
        AND version = 'v2'
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address AS factory_address,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    LOWER(CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40))) AS token0,
    LOWER(CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))) AS token1,
    CASE
        WHEN RIGHT(
            topics [3] :: STRING,
            1
        ) = '0' THEN FALSE
        ELSE TRUE
    END AS stable,
    CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS pool_address,
    m.protocol,
    m.version,
    CONCAT(
        m.protocol,
        '-',
        m.version
    ) AS platform,
    'PairCreated' AS event_name,
    CONCAT(
        tx_hash :: STRING,
        '-',
        event_index :: STRING
    ) AS _log_id,
    modified_timestamp
FROM
    {{ ref('core__fact_event_logs') }}
WHERE
    topics [0] = '0x2128d88d14c80cb081c1252a5acff7a264671bf199ce226b53788fb26065005e'
    AND contract_address IN (
        SELECT
            contract_address
        FROM
            contract_mapping
        WHERE type = 'factory'
    )
    AND pool_address NOT IN (
        SELECT
            contract_address
        FROM
            contract_mapping
        WHERE type = 'converter'
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

qualify(ROW_NUMBER() over (PARTITION BY pool_address
ORDER BY
    modified_timestamp DESC)) = 1
