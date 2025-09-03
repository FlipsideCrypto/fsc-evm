{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends on: {{ ref('bronze__balances_erc20') }}
{{ config(
    materialized = 'incremental',
    unique_key = 'fact_balances_native_daily_id',
    cluster_by = ['block_date','_inserted_timestamp::date'],
    incremental_predicates = ["dynamic_range", "block_number"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    merge_exclude_columns = ["inserted_timestamp"],
    full_refresh = vars.GLOBAL_GOLD_FR_ENABLED,
    tags = ['gold','balances','native','phase_4']
) }}

SELECT
    VALUE :"BLOCK_NUMBER" :: NUMBER AS block_number,
    VALUE :"BLOCK_DATE_UNIX" :: TIMESTAMP AS block_date,
    VALUE :"ADDRESS" :: STRING AS address,
    TRY_TO_NUMBER(
        utils.udf_hex_to_int(
            DATA :result :: STRING
        )
    ) AS balance,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number','address']
    ) }} AS fact_balances_native_daily_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__balances_native') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
    AND DATA :result :: STRING <> '0x'
{% else %}
    {{ ref('bronze__balances_native_fr') }}
WHERE
    DATA :result :: STRING <> '0x'
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY fact_balances_native_daily_id
ORDER BY
    _inserted_timestamp DESC)) = 1
