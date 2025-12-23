{# Get variables #}
{% set vars = return_vars() %}
{# Log configuration details #}
{{ log_model_details() }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'aave_v1_tvl_id',
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
                        {{ ref('silver_reads__aave_v1_reads') }}
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
balances AS (
    SELECT
        block_number,
        block_date,
        '0x0000000000000000000000000000000000000000' AS contract_address, -- Represents native asset, for pricing purposes
        address,
        balance_hex AS amount_hex,
        balance_raw AS amount_raw,
        'aave' AS protocol,
        'v1' AS version,
        CONCAT(
            protocol,
            '-',
            version
        ) AS platform,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('balances__ez_balances_native_daily') }}
    WHERE
        address = '0x3dfd23a6c5e8bbcfc9581d2e864a68feb6a076d3'
        AND balance_raw IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
FINAL AS (
    SELECT
        *
    FROM
        reads
    UNION ALL
    SELECT
        *
    FROM
        balances
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
    ) }} AS aave_v1_tvl_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY aave_v1_tvl_id
ORDER BY
    modified_timestamp DESC)) = 1
