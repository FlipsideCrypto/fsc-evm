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
        C.block_number,
        C.block_date,
        C.contract_address,
        C.address,
        result_hex AS amount_hex,
        IFNULL(
            CASE
                WHEN LENGTH(amount_hex) <= 4300
                AND amount_hex IS NOT NULL THEN TRY_CAST(utils.udf_hex_to_int(amount_hex) AS bigint)END,
                CASE
                    WHEN amount_hex IS NOT NULL THEN TRY_CAST(utils.udf_hex_to_int(RTRIM(amount_hex, '0')) AS bigint)
                END
            ) AS amount_raw,
            C.protocol,
            C.version,
            C.platform,
            C._inserted_timestamp
            FROM
                {{ ref('silver__contract_reads') }} C
                LEFT JOIN {{ ref('silver_reads__aave_v3_reads') }}
                r
                ON C.contract_address = r.contract_address
            WHERE
                r.contract_address IS NOT NULL
                AND amount_raw IS NOT NULL

{% if is_incremental() %}
AND C.modified_timestamp >= (
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
    amount_hex,
    amount_raw,
    protocol,
    version,
    platform,
    {{ dbt_utils.generate_surrogate_key(
        ['block_date','contract_address','platform']
    ) }} AS aave_v3_tvl_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    reads
