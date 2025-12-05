{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('bronze__stablecoin_reads') }}
-- depends_on: {{ ref('price__ez_asset_metadata') }}
{{ config(
    materialized = 'incremental',
    unique_key = 'stablecoin_reads_id',
    cluster_by = ['block_date'],
    incremental_predicates = ["dynamic_range", "block_number"],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "{{ unverify_stablecoins() }}",
    full_refresh = vars.GLOBAL_SILVER_FR_ENABLED,
    tags = ['silver','defi','stablecoins','curated_daily']
) }}

WITH stablecoin_reads AS (

    SELECT
        VALUE :"BLOCK_NUMBER" :: NUMBER AS block_number,
        (
            VALUE :"BLOCK_DATE_UNIX" :: TIMESTAMP
        ) :: DATE AS block_date,
        contract_address,
        PARSE_JSON(
            VALUE :"METADATA_STR" :: STRING
        ) :: variant AS metadata,
        DATA :result :: STRING AS result_hex,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__stablecoin_reads') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(MAX(_inserted_timestamp), '1970-01-01')
        FROM
            {{ this }})
            AND DATA :result :: STRING <> '0x'
        {% else %}
            {{ ref('bronze__stablecoin_reads_fr') }}
        WHERE
            DATA :result :: STRING <> '0x'
        {% endif %}

        qualify(ROW_NUMBER() over (PARTITION BY block_number, contract_address
        ORDER BY
            _inserted_timestamp DESC)) = 1
    ),
results AS (
    SELECT
        block_number,
        block_date,
        contract_address,
        metadata,
        IFF(
            C.decimals IS NULL,
            18,
            C.decimals
        ) AS decimals_adj,
        result_hex AS amount_hex,
        IFNULL(
            CASE
                WHEN LENGTH(amount_hex) <= 4300
                AND amount_hex IS NOT NULL THEN TRY_CAST(utils.udf_hex_to_int(amount_hex) AS bigint)END,
                CASE
                    WHEN amount_hex IS NOT NULL THEN TRY_CAST(utils.udf_hex_to_int(RTRIM(amount_hex, '0')) AS bigint)
                END
            ) AS amount_raw,
            IFF(
                decimals_adj IS NULL,
                NULL,
                utils.udf_decimal_adjust(
                    amount_raw,
                    decimals_adj
                )
            ) AS amount_precise,
            amount_precise :: FLOAT AS amount,
            _inserted_timestamp
            FROM
                stablecoin_reads s
                LEFT JOIN {{ ref('core__dim_contracts') }} C
                ON s.contract_address = C.address
            WHERE
                amount_raw IS NOT NULL
        )
SELECT
    block_number,
    block_date,
    contract_address,
    metadata,
    decimals_adj AS decimals,
    amount_hex,
    amount_raw,
    amount_precise,
    amount,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_date','contract_address']
    ) }} AS stablecoin_reads_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    results
