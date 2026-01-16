{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'sky_v1_tvl_id',
    tags = ['silver','defi','tvl','curated_daily']
) }}

WITH reads AS (
    SELECT
        block_number,
        block_date,
        contract_address,
        address,
        result_hex AS amount_hex,
        IFNULL(
            CASE
                WHEN LENGTH(result_hex) <= 4300
                AND result_hex IS NOT NULL THEN TRY_CAST(utils.udf_hex_to_int(result_hex) AS BIGINT)
            END,
            CASE
                WHEN result_hex IS NOT NULL THEN TRY_CAST(utils.udf_hex_to_int(RTRIM(result_hex, '0')) AS BIGINT)
            END
        ) AS amount_raw,
        protocol,
        version,
        platform,
        modified_timestamp
    FROM
        {{ ref('silver__contract_reads') }}
    WHERE
        platform = 'sky-v1'
        AND result_hex IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp > (
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
    contract_address AS token_address,
    amount_hex,
    amount_raw,
    protocol,
    version,
    platform,
    {{ dbt_utils.generate_surrogate_key(['block_date','contract_address','address','platform']) }} AS sky_v1_tvl_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    reads
WHERE
    amount_raw IS NOT NULL
    AND amount_raw > 0
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY sky_v1_tvl_id
    ORDER BY modified_timestamp DESC
) = 1
