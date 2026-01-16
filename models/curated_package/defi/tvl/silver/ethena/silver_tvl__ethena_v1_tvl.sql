{# Get variables #}
{% set vars = return_vars() %}
{# Log configuration details #}
{{ log_model_details() }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'ethena_v1_tvl_id',
    tags = ['silver','defi','tvl','curated_daily']
) }}
-- Ethena TVL: totalSupply of USDe (sourced from stablecoins reads)
-- USDe: 0x4c9edd5852cd905f086c759e8383e09bff1e68b3
WITH reads AS (

    SELECT
        block_number,
        block_date,
        contract_address,
        result_hex AS amount_hex,
        IFNULL(
            CASE
                WHEN LENGTH(result_hex) <= 4300
                AND result_hex IS NOT NULL THEN TRY_CAST(utils.udf_hex_to_int(result_hex) AS bigint)END,
                CASE
                    WHEN result_hex IS NOT NULL THEN TRY_CAST(utils.udf_hex_to_int(RTRIM(result_hex, '0')) AS bigint)
                END
            ) AS amount_raw,
            _inserted_timestamp
            FROM
                {{ ref('silver__contract_reads') }}
            WHERE
                platform = 'stablecoins-v1'
                AND function_name = 'totalSupply'
                AND LOWER(contract_address) = LOWER('0x4c9edd5852cd905f086c759e8383e09bff1e68b3') -- USDe
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
    NULL AS address,
    contract_address AS token_address,
    amount_hex,
    amount_raw,
    'ethena' AS protocol,
    'v1' AS version,
    CONCAT(
        protocol,
        '-',
        version
    ) AS platform,
    {{ dbt_utils.generate_surrogate_key(['block_date','contract_address','address','platform']) }} AS ethena_v1_tvl_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    reads
WHERE
    amount_raw IS NOT NULL
    AND amount_raw > 0 qualify ROW_NUMBER() over (
        PARTITION BY ethena_v1_tvl_id
        ORDER BY
            modified_timestamp DESC
    ) = 1
