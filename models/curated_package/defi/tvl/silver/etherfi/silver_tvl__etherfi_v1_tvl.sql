{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'etherfi_v1_tvl_id',
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
            ) AS amount_raw_unadj,
            metadata :token_address :: STRING AS token_address,
            metadata :attribution :: STRING AS attribution,
            metadata :chain :: STRING AS chain,
            protocol,
            version,
            platform,
            _inserted_timestamp
            FROM
                {{ ref('silver__contract_reads') }}
            WHERE
                platform = 'etherfi-v1'
                AND chain = '{{ vars.GLOBAL_PROJECT_NAME }}'
                AND amount_raw_unadj IS NOT NULL

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
    CASE 
        WHEN r.token_address IS NOT NULL 
        THEN r.token_address ELSE r.contract_address 
    END AS contract_address,
    CASE 
        WHEN r.token_address IS NOT NULL AND r.address IS NULL
        THEN r.contract_address ELSE r.address 
    END AS address,
    amount_hex,
    CASE
        WHEN r.contract_address = '0xab7590cee3ef1a863e9a5877fbb82d9be11504da' THEN -1 * amount_raw_unadj 
        --categoryTVL() must be subtracted from getTVL(), negative value enables SUM
        ELSE amount_raw_unadj
    END AS amount_raw,
    protocol,
    version,
    platform,
    attribution,
    chain,
    {{ dbt_utils.generate_surrogate_key(
        ['block_date','contract_address','address','platform']
    ) }} AS etherfi_v1_tvl_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    reads r qualify(ROW_NUMBER() over(PARTITION BY etherfi_v1_tvl_id
ORDER BY
    modified_timestamp DESC)) = 1
