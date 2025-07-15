{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'pool_address',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_dex','defi','dex','curated']
) }}

WITH pool_traces AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        to_address AS pool_address,
        to_address AS contract_address,
        regexp_substr_all(SUBSTR(input, 11, len(input)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [0] :: STRING)) / 32 AS token_index, 
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [1] :: STRING)) / 32 AS decimal_index,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [token_index] :: STRING
            )
        ) AS token_number,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [decimal_index] :: STRING
            )
        ) AS decimals_number,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [5] :: STRING)) * pow(
            10,
            -10
        ) AS swap_fee,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [6] :: STRING)) * pow(
            10,
            -10
        ) AS admin_fee,
        -- 50% of swap fee
        CONCAT('0x', SUBSTR(segmented_data [7] :: STRING, 25, 40)) AS lp_token,
        CONCAT(
            '0x',
            SUBSTR(
                segmented_data [token_index+1] :: STRING,
                25,
                40
            )
        ) AS token0,
        CONCAT(
            '0x',
            SUBSTR(
                segmented_data [token_index+2] :: STRING,
                25,
                40
            )
        ) AS token1,
        CASE
            WHEN token_number > 2 THEN CONCAT(
                '0x',
                SUBSTR(
                    segmented_data [token_index+3] :: STRING,
                    25,
                    40
                )
            )
            ELSE NULL
        END AS token2,
        CASE
            WHEN token_number > 3 THEN CONCAT(
                '0x',
                SUBSTR(
                    segmented_data [token_index+4] :: STRING,
                    25,
                    40
                )
            )
            ELSE NULL
        END AS token3,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [decimal_index+1] :: STRING
            )
        ) AS decimal0,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [decimal_index+2] :: STRING
            )
        ) AS decimal1,
        CASE
            WHEN decimals_number > 2 THEN TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    segmented_data [decimal_index+3] :: STRING
                )
            )
            ELSE NULL
        END AS decimal2,
        CASE
            WHEN decimals_number > 3 THEN TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    segmented_data [decimal_index+4] :: STRING
                )
            )
            ELSE NULL
        END AS decimal3,
        utils.udf_hex_to_string(
            segmented_data [array_size(segmented_data)-3] :: STRING
        ) AS lp_name,
        utils.udf_hex_to_string(
            segmented_data [array_size(segmented_data)-1] :: STRING
        ) AS lp_symbol,
        CONCAT(
            tx_hash :: STRING,
            '-',
            trace_index :: STRING -- using trace_index instead of event_index
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        1 = 1
        AND origin_function_signature = '0xb28cb6dc'
        AND LEFT(
            input,
            10
        ) = '0xb28cb6dc'
        AND trace_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    token0,
    token1,
    token2,
    token3,
    decimal0,
    decimal1,
    decimal2,
    decimal3,
    lp_name,
    lp_symbol,
    swap_fee,
    admin_fee,
    lp_token,
    _log_id,
    _inserted_timestamp
FROM
    pool_traces qualify(ROW_NUMBER() over (PARTITION BY pool_address
ORDER BY
    _inserted_timestamp DESC)) = 1
