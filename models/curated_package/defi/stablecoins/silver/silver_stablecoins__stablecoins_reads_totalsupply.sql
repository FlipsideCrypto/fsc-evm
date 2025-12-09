{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('bronze__contract_reads') }}
-- depends_on: {{ ref('price__ez_asset_metadata') }}
{{ config(
    materialized = 'incremental',
    unique_key = 'stablecoins_reads_totalsupply_id',
    cluster_by = ['block_date'],
    incremental_predicates = ["dynamic_range", "block_number"],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "{{ unverify_stablecoins() }}",
    full_refresh = vars.GLOBAL_SILVER_FR_ENABLED,
    tags = ['silver','defi','stablecoins','heal','curated_daily']
) }}

WITH stablecoin_reads AS (

    SELECT
        block_number,
        block_date,
        contract_address,
        metadata,
        result_hex,
        _inserted_timestamp
    FROM
        {{ ref('silver__contract_reads') }}

{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
results AS (
    SELECT
        block_number,
        block_date,
        contract_address,
        metadata,
        metadata :decimals AS decimals,
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
                decimals IS NULL,
                NULL,
                utils.udf_decimal_adjust(
                    amount_raw,
                    decimals
                )
            ) AS amount_precise,
            amount_precise :: FLOAT AS amount,
            _inserted_timestamp
            FROM
                stablecoin_reads
            WHERE
                amount_raw IS NOT NULL
        ),

{% if is_incremental() %}
heal_model AS (
    SELECT
        t.block_number,
        t.block_date,
        t.contract_address,
        t.metadata,
        d.decimals AS decimals_heal,
        t.amount_hex,
        t.amount_raw,
        IFF(
            decimals_heal IS NULL,
            NULL,
            utils.udf_decimal_adjust(
                t.amount_raw,
                decimals_heal
            )
        ) AS amount_precise_heal,
        amount_precise_heal :: FLOAT AS amount_heal,
        t._inserted_timestamp
    FROM
        {{ this }}
        t
        INNER JOIN {{ ref('defi__dim_stablecoins') }}
        d
        ON t.contract_address = d.contract_address
    WHERE
        t.decimals IS NULL
        AND d.decimals IS NOT NULL
),
{% endif %}

FINAL AS (
    SELECT
        *
    FROM
        results

{% if is_incremental() %}
UNION ALL
SELECT
    block_number,
    block_date,
    contract_address,
    metadata,
    decimals_heal AS decimals,
    amount_hex,
    amount_raw,
    amount_precise_heal AS amount_precise,
    amount_heal AS amount,
    _inserted_timestamp
FROM
    heal_model
{% endif %}
)
SELECT
    block_number,
    block_date,
    contract_address,
    metadata,
    decimals,
    amount_hex,
    amount_raw,
    amount_precise,
    amount,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_date','contract_address']
    ) }} AS stablecoins_reads_totalsupply_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL

{% if is_incremental() %}
qualify(ROW_NUMBER() over (PARTITION BY stablecoins_reads_totalsupply_id
ORDER BY
    modified_timestamp DESC)) = 1
{% endif %}
