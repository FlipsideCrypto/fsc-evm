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
        protocol = 'hashflow'
        AND version = 'v1'
),
pools AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        from_address AS deployer_address,
        to_address AS contract_address,
        m.protocol,
        m.version,
        CONCAT(
            m.protocol,
            '-',
            m.version
        ) AS platform,
        concat_ws(
            '-',
            block_number,
            tx_position,
            CONCAT(
                t.TYPE,
                '_',
                trace_address
            )
        ) AS _call_id,
        modified_timestamp
    FROM
        {{ ref('core__fact_traces') }}
        t
        INNER JOIN contract_mapping m
        ON t.from_address = m.contract_address
    WHERE
        t.TYPE ILIKE 'create%'
        AND tx_succeeded
        AND trace_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY to_address
ORDER BY
    block_timestamp ASC)) = 1
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    deployer_address,
    contract_address AS pool_address,
    platform,
    protocol,
    version,
    _call_id,
    modified_timestamp
FROM
    pools
