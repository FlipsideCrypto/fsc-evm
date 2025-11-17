{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('bronze__curated_reads') }}
{{ config(
    materialized = 'incremental',
    unique_key = 'curated_reads_id',
    cluster_by = ['block_date'],
    incremental_predicates = ["dynamic_range", "block_number"],
    merge_exclude_columns = ["inserted_timestamp"],
    full_refresh = vars.GLOBAL_SILVER_FR_ENABLED,
    tags = ['silver','curated_reads','curated_daily']
) }}

SELECT
    VALUE :"ADDRESS" :: STRING AS address,
    VALUE :"FUNCTION_NAME" :: STRING AS function_name,
    VALUE :"FUNCTION_SIG" :: STRING AS function_sig,
    VALUE :"INPUT" :: STRING AS input,
    VALUE :"BLOCK_NUMBER" :: NUMBER AS block_number,
    (
        VALUE :"BLOCK_DATE_UNIX" :: TIMESTAMP
    ) :: DATE AS block_date,
    DATA :result :: STRING AS result_hex,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number','contract_address', 'address', 'function_name', 'function_sig', 'input']
    ) }} AS curated_reads_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__curated_reads') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(MAX(_inserted_timestamp), '1970-01-01')
        FROM
            {{ this }})
            AND DATA :result :: STRING <> '0x'
        {% else %}
            {{ ref('bronze__curated_reads_fr') }}
        WHERE
            DATA :result :: STRING <> '0x'
        {% endif %}

        qualify(ROW_NUMBER() over (PARTITION BY curated_reads_id
        ORDER BY
            _inserted_timestamp DESC)) = 1
