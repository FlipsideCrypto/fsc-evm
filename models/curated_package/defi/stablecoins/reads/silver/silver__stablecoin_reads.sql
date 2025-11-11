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
    tags = ['silver','defi','stablecoins','curated']
) }}

WITH stablecoin_reads AS (

    SELECT
        VALUE :"BLOCK_NUMBER" :: NUMBER AS block_number,
        (
            VALUE :"BLOCK_DATE_UNIX" :: TIMESTAMP
        ) :: DATE AS block_date,
        contract_address,
        DATA :result :: STRING AS total_supply_hex,
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
total_supply AS (
    SELECT
        block_number,
        block_date,
        contract_address,
        IFF(
            C.decimals IS NULL,
            18,
            C.decimals
        ) AS decimals_adj,
        total_supply_hex,
        IFNULL(
            CASE
                WHEN LENGTH(total_supply_hex) <= 4300
                AND total_supply_hex IS NOT NULL THEN TRY_CAST(utils.udf_hex_to_int(total_supply_hex) AS bigint)END,
                CASE
                    WHEN total_supply_hex IS NOT NULL THEN TRY_CAST(utils.udf_hex_to_int(RTRIM(total_supply_hex, '0')) AS bigint)
                END
            ) AS total_supply_raw,
            IFF(
                decimals_adj IS NULL,
                NULL,
                utils.udf_decimal_adjust(
                    total_supply_raw,
                    decimals_adj
                )
            ) AS total_supply_precise,
            total_supply_precise :: FLOAT AS total_supply,
            _inserted_timestamp
            FROM
                stablecoin_reads s
                LEFT JOIN {{ ref('core__dim_contracts') }} C
                ON s.contract_address = C.address
            WHERE
                total_supply_raw IS NOT NULL
        )
SELECT
    block_number,
    block_date,
    contract_address,
    decimals_adj AS decimals,
    total_supply_hex,
    total_supply_raw,
    total_supply_precise,
    total_supply,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_date','contract_address']
    ) }} AS stablecoin_reads_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    total_supply
