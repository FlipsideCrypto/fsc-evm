{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'uniswap_v4_tvl_id',
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
                WHEN LENGTH(amount_hex) <= 4300
                AND amount_hex IS NOT NULL THEN TRY_CAST(utils.udf_hex_to_int(amount_hex) AS bigint)END,
                CASE
                    WHEN amount_hex IS NOT NULL THEN TRY_CAST(utils.udf_hex_to_int(RTRIM(amount_hex, '0')) AS bigint)
                END
            ) AS amount_raw,
            metadata :address_type :: STRING AS address_type,
            metadata :hook_address :: STRING AS hook_address,
            metadata :token0 :: STRING AS token_0_address,
            metadata :token1 :: STRING AS token_1_address,
            protocol,
            version,
            platform,
            _inserted_timestamp
            FROM
                {{ ref('silver__contract_reads') }}
            WHERE
                platform = 'uniswap-v4'
                AND amount_raw IS NOT NULL

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
    amount_hex,
    amount_raw,
    address_type,
    hook_address,
    token_0_address,
    token_1_address,
    protocol,
    version,
    platform,
    {{ dbt_utils.generate_surrogate_key(
        ['block_date','contract_address','address','platform']
    ) }} AS uniswap_v4_tvl_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    reads qualify(ROW_NUMBER() over(PARTITION BY uniswap_v4_tvl_id
ORDER BY
    modified_timestamp DESC)) = 1
