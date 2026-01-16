{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('streamline__contract_reads_records') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'uniswap_v2_tvl_id',
    post_hook = '{{ unverify_tvl() }}',
    tags = ['silver','defi','tvl','heal','curated_daily']
) }}

WITH reads AS (

    SELECT
        block_number,
        block_date,
        contract_address AS pool_address,
        address, --NULL
        regexp_substr_all(SUBSTR(result_hex, 3, len(result_hex)), '.{64}') AS segmented_data,
        segmented_data [0] :: STRING AS reserve_0_hex,
        segmented_data [1] :: STRING AS reserve_1_hex,
        segmented_data [2] :: STRING AS block_timestamp_last_hex,
        IFNULL(
            CASE
                WHEN LENGTH(reserve_0_hex) <= 4300
                AND reserve_0_hex IS NOT NULL THEN TRY_CAST(utils.udf_hex_to_int(reserve_0_hex) AS bigint)END,
                CASE
                    WHEN reserve_0_hex IS NOT NULL THEN TRY_CAST(utils.udf_hex_to_int(RTRIM(reserve_0_hex, '0')) AS bigint)
                END
            ) AS reserve_0_raw,
            IFNULL(
                CASE
                    WHEN LENGTH(reserve_1_hex) <= 4300
                    AND reserve_1_hex IS NOT NULL THEN TRY_CAST(utils.udf_hex_to_int(reserve_1_hex) AS bigint)END,
                    CASE
                        WHEN reserve_1_hex IS NOT NULL THEN TRY_CAST(utils.udf_hex_to_int(RTRIM(reserve_1_hex, '0')) AS bigint)
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
                    metadata :token0 :: STRING AS token_0_address,
                    metadata :token1 :: STRING AS token_1_address,
                    protocol,
                    version,
                    platform,
                    _inserted_timestamp
                    FROM
                        {{ ref('silver__contract_reads') }}
                    WHERE
                        reserve_0_raw IS NOT NULL
                        AND reserve_1_raw IS NOT NULL
                        AND block_timestamp_last_raw IS NOT NULL
                        AND platform IN (
                            SELECT DISTINCT platform
                            FROM {{ ref('silver_reads__uniswap_v2_reads') }}
                        )

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
all_reads AS (
SELECT
    block_number,
    block_date,
    token_0_address AS contract_address,
    pool_address AS address,
    reserve_0_hex AS amount_hex,
    reserve_0_raw AS amount_raw,
    protocol,
    version,
    platform
FROM reads
UNION ALL
SELECT
    block_number,
    block_date,
    token_1_address AS contract_address,
    pool_address AS address,
    reserve_1_hex AS amount_hex,
    reserve_1_raw AS amount_raw,
    protocol,
    version,
    platform
FROM reads
)
SELECT
    block_number,
    block_date,
    contract_address,
    address,
    contract_address AS token_address,
    amount_hex,
    amount_raw,
    protocol,
    version,
    platform,
    {{ dbt_utils.generate_surrogate_key(
        ['block_date','contract_address','address','platform']
    ) }} AS uniswap_v2_tvl_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_reads qualify(ROW_NUMBER() over(PARTITION BY uniswap_v2_tvl_id
ORDER BY
    modified_timestamp DESC)) = 1
