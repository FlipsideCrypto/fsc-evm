{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'tornado_cash_tvl_id',
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
                platform = 'tornado_cash-v1'
                AND amount_raw IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
balances AS (
    SELECT
        b.block_number,
        b.block_date,
        t.token_address AS contract_address,
        b.address AS address,
        balance_hex AS amount_hex,
        balance_raw AS amount_raw,
        'tornado_cash' AS protocol,
        'v1' AS version,
        CONCAT(
            protocol,
            '-',
            version
        ) AS platform,
        b.modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('balances__ez_balances_native_daily') }}
        b
        LEFT JOIN {{ ref('silver_reads__tornado_cash_mixer_seed') }}
        t
        ON b.address = t.mixer_address
    WHERE
        t.mixer_address IS NOT NULL
        AND t.token_address = '0x0000000000000000000000000000000000000000'
        AND t.chain = '{{ vars.GLOBAL_PROJECT_NAME }}'
        AND balance_raw IS NOT NULL

{% if is_incremental() %}
AND b.modified_timestamp > (
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
    amount_hex,
    amount_raw,
    protocol,
    version,
    platform,
    {{ dbt_utils.generate_surrogate_key(
        ['block_date','contract_address','address','platform']
    ) }} AS tornado_cash_tvl_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
