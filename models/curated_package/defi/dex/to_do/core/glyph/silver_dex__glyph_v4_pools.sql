{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'pool_address',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_dex','defi','dex','curated']
) }}

WITH pool_creation AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS token0,
        CONCAT('0x', SUBSTR(topic_2, 27, 40)) AS token1,
        CONCAT('0x', SUBSTR(DATA, 27, 40)) AS pool_address,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address = '0x74efe55bea4988e7d92d03efd8ddb8bf8b7bd597'
        AND topic_0 = '0x91ccaa7a278130b65168c3a0c8d3bcae84cf5e43704342bd3ec0b59e59c036db'
        AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
initial_info AS (
    SELECT
        tx_hash,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        utils.udf_hex_to_int('s2c', CONCAT('0x', segmented_data [0] :: STRING)) AS price,
        utils.udf_hex_to_int('s2c', CONCAT('0x', segmented_data [1] :: STRING)) AS tick
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING = '0x98636036cb66a9c19a37435efc1e90142190214e8abeb821bdba3f2990dd4c95'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                pool_creation
        )
        AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
tick_spacing AS (
    SELECT
        tx_hash,
        contract_address,
        utils.udf_hex_to_int(
            's2c',
            DATA :: STRING
        ) :: INTEGER AS tick_spacing
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topic_0 = '0x01413b1d5d4c359e9a0daa7909ecda165f6e8c51fe2ff529d74b22a5a7c02645'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                pool_creation
        )
        AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
fee AS (
    SELECT
        tx_hash,
        contract_address,
        utils.udf_hex_to_int(
            's2c',
            DATA :: STRING
        ) :: INTEGER AS fee
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topic_0 = '0x598b9f043c813aa6be3426ca60d1c65d17256312890be5118dab55b0775ebe2a'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                pool_creation
        )
        AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    p.tx_hash,
    p.contract_address,
    event_index,
    token0,
    token1,
    pool_address,
    fee,
    tick,
    tick_spacing,
    _log_id,
    _inserted_timestamp
FROM
    pool_creation p
    INNER JOIN initial_info
    ON initial_info.contract_address = p.pool_address
    INNER JOIN tick_spacing
    ON tick_spacing.contract_address = p.pool_address
    INNER JOIN fee
    ON fee.contract_address = p.pool_address qualify(ROW_NUMBER() over (PARTITION BY pool_address
ORDER BY
    _inserted_timestamp DESC)) = 1
