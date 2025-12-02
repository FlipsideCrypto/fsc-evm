{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'uniswap_v2_tvl_id',
    tags = ['silver','defi','tvl','curated_daily']
) }}

WITH reads AS (

    SELECT
        r.block_number,
        r.block_date,
        r.contract_address,
        r.address,
        regexp_substr_all(SUBSTR(result_hex, 3, len(result_hex)), '.{64}') AS segmented_data,
        segmented_data [0] :: STRING AS reserve_0_hex,
        segmented_data [1] :: STRING AS reserve_1_hex,
        segmented_data [2] :: STRING AS block_timestamp_last_hex,
        IFNULL(
            CASE
                WHEN LENGTH(reserve_0_hex) <= 4300
                AND reserve_0_hex IS NOT NULL THEN TRY_CAST(utils.udf_hex_to_int(reserve_0_hex) AS FLOAT)END,
                CASE
                    WHEN reserve_0_hex IS NOT NULL THEN TRY_CAST(utils.udf_hex_to_int(RTRIM(reserve_0_hex, '0')) AS FLOAT)
                END
            ) AS reserve_0_raw,
            IFNULL(
                CASE
                    WHEN LENGTH(reserve_1_hex) <= 4300
                    AND reserve_1_hex IS NOT NULL THEN TRY_CAST(utils.udf_hex_to_int(reserve_1_hex) AS FLOAT)END,
                    CASE
                        WHEN reserve_1_hex IS NOT NULL THEN TRY_CAST(utils.udf_hex_to_int(RTRIM(reserve_1_hex, '0')) AS FLOAT)
                    END
                ) AS reserve_1_raw,
                IFNULL(
                    CASE
                        WHEN LENGTH(block_timestamp_last_hex) <= 4300
                        AND block_timestamp_last_hex IS NOT NULL THEN TRY_CAST(utils.udf_hex_to_int(block_timestamp_last_hex) AS TIMESTAMP)END,
                        CASE
                            WHEN block_timestamp_last_hex IS NOT NULL THEN TRY_CAST(
                                utils.udf_hex_to_int(RTRIM(block_timestamp_last_hex, '0')) AS TIMESTAMP
                            )
                        END
                    ) AS block_timestamp_last_raw,
                    p.token0 AS token_0_address,
                    p.token1 AS token_1_address,
                    r.protocol,
                    r.version,
                    r.platform,
                    r._inserted_timestamp
                    FROM
                        {{ ref('silver__contract_reads') }}
                        r
                        INNER JOIN {{ ref('silver_dex__paircreated_evt_v2_pools') }}
                        p
                        ON r.contract_address = p.pool_address
                    WHERE
                        reserve_0_raw IS NOT NULL
                        AND reserve_1_raw IS NOT NULL
                        AND block_timestamp_last_raw IS NOT NULL

{% if is_incremental() %}
AND r.modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_date,
    contract_address,
    address,
    reserve_0_hex,
    reserve_1_hex,
    block_timestamp_last_hex,
    reserve_0_raw,
    reserve_1_raw,
    block_timestamp_last_raw,
    token_0_address,
    token_1_address,
    protocol,
    version,
    platform,
    {{ dbt_utils.generate_surrogate_key(
        ['block_date','contract_address','platform']
    ) }} AS uniswap_v2_tvl_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    reads
