{# Get variables #}
{% set vars = return_vars() %}
{# Log configuration details #}
{{ log_model_details() }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'aave_v3_tvl_id',
    tags = ['silver','defi','tvl','curated_daily']
) }}

WITH reads AS (

    SELECT
        block_number,
        block_date,
        contract_address AS atoken_address,
        address,
        result_hex AS amount_hex,
        IFNULL(
            CASE
                WHEN LENGTH(amount_hex) <= 4300
                AND amount_hex IS NOT NULL THEN TRY_CAST(utils.udf_hex_to_int(amount_hex) AS bigint)END,
                CASE
                    WHEN amount_hex IS NOT NULL THEN TRY_CAST(utils.udf_hex_to_int(RTRIM(amount_hex, '0')) AS bigint)
                END
            ) AS amount_raw,
            metadata :underlying_address :: STRING AS underlying_address,
            protocol,
            version,
            platform,
            _inserted_timestamp
            FROM
                {{ ref('silver__contract_reads') }}
            WHERE
                amount_raw IS NOT NULL
                AND platform IN (
                    SELECT
                        DISTINCT platform
                    FROM
                        {{ ref('silver_reads__aave_v3_reads') }}
                )

{% if is_incremental() %}
AND modified_timestamp >= (
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
    underlying_address AS contract_address,
    atoken_address AS address,
    amount_hex,
    amount_raw,
    protocol,
    version,
    platform,
    12 AS usd_threshold,
    {{ dbt_utils.generate_surrogate_key(
        ['block_date','contract_address','platform']
    ) }} AS aave_v3_tvl_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    reads
