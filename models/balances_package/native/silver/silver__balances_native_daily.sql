{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends on: {{ ref('bronze__balances_native') }}
{{ config(
    materialized = 'incremental',
    unique_key = 'balances_native_daily_id',
    cluster_by = ['block_date'],
    incremental_predicates = ["dynamic_range", "block_number"],
    merge_exclude_columns = ["inserted_timestamp"],
    full_refresh = vars.GLOBAL_SILVER_FR_ENABLED,
    tags = ['silver','balances','native','phase_4']
) }}

SELECT
    VALUE :"BLOCK_NUMBER" :: NUMBER AS block_number,
    (
        VALUE :"BLOCK_DATE_UNIX" :: TIMESTAMP
    ) :: DATE AS block_date,
    VALUE :"ADDRESS" :: STRING AS address,
    DATA :result :: STRING AS balance_hex,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_date','address']
    ) }} AS balances_native_daily_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__balances_native') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
    AND DATA :result :: STRING <> '0x'
{% else %}
    {{ ref('bronze__balances_native_fr') }}
WHERE
    DATA :result :: STRING <> '0x'
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY balances_native_daily_id
ORDER BY
    _inserted_timestamp DESC)) = 1
