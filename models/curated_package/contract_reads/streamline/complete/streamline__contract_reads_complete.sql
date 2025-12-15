{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends on: {{ ref('bronze__contract_reads') }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'incremental',
    unique_key = 'contract_reads_complete_id',
    incremental_predicates = ['dynamic_range', 'partition_key'],
    full_refresh = vars.GLOBAL_STREAMLINE_FR_ENABLED,
    tags = ['streamline','contract_reads','complete','phase_4']
) }}

SELECT
    partition_key,
    contract_address,
    VALUE :"ADDRESS" :: STRING AS address,
    VALUE :"BLOCK_NUMBER" :: NUMBER AS block_number,
    (
        VALUE :"BLOCK_DATE_UNIX" :: TIMESTAMP
    ) :: DATE AS block_date,
    VALUE :"FUNCTION_NAME" :: STRING AS function_name,
    VALUE :"FUNCTION_SIG" :: STRING AS function_sig,
    VALUE :"INPUT" :: STRING AS input,
    PARSE_JSON(
        VALUE :"METADATA_STR" :: STRING
    ) :: variant AS metadata,
    VALUE :"PROTOCOL" :: STRING AS protocol,
    VALUE :"VERSION" :: STRING AS version,
    VALUE :"PLATFORM" :: STRING AS platform,
    VALUE :"TYPE" :: STRING AS type,
    file_name,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address', 'address', 'block_number', 'function_name', 'function_sig', 'input', 'platform']
    ) }} AS contract_reads_complete_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__contract_reads') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE (MAX(_inserted_timestamp), '1970-01-01' :: TIMESTAMP)
        FROM
            {{ this }})
        {% else %}
            {{ ref('bronze__contract_reads_fr') }}
        {% endif %}

        qualify(ROW_NUMBER() over (PARTITION BY contract_reads_complete_id
        ORDER BY
            _inserted_timestamp DESC)) = 1
