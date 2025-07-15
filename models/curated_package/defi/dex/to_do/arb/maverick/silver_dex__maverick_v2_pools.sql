{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'pool_address',
    tags = ['silver_dex','defi','dex','curated']
) }}

WITH created_pools AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS pool_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [1] :: STRING
            )
        ) AS protocolFeeRatio,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [2] :: STRING
            )
        ) AS feeAIn,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [3] :: STRING
            )
        ) AS feeBIn,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [4] :: STRING
            )
        ) AS tickSpacing,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [5] :: STRING
            )
        ) AS lookback,
        -- lookback period
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [6] :: STRING
            )
        ) AS activetick,
        -- pool type (static/right/left/both/all)
        CONCAT(
            '0x',
            SUBSTR(
                segmented_data [7] :: STRING,
                25,
                40
            )
        ) AS tokenA,
        CONCAT(
            '0x',
            SUBSTR(
                segmented_data [8] :: STRING,
                25,
                40
            )
        ) AS tokenB,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [9] :: STRING
            )
        ) AS kinds,
        CONCAT(
            '0x',
            SUBSTR(
                segmented_data [10] :: STRING,
                25,
                40
            )
        ) AS accessor,
        -- null if permissionless pool
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address = LOWER('0x0A7e848Aca42d879EF06507Fca0E7b33A0a63c1e') --factory
        AND topic_0 = '0x848331e408557f4b7eb6561ca1c18a3ac43004fbe64b8b5bce613855cfdf22d2' --paircreated
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
    contract_address,
    pool_address,
    protocolFeeRatio AS protocol_fee_ratio,
    feeAin,
    feeBin,
    tickspacing AS tick_spacing,
    lookback,
    activetick,
    tokenA,
    tokenB,
    kinds,
    accessor,
    _log_id,
    modified_timestamp
FROM
    created_pools qualify(ROW_NUMBER() over(PARTITION BY pool_address
ORDER BY
    modified_timestamp DESC)) = 1
