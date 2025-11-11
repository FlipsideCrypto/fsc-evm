{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends on: {{ ref('bronze__stablecoin_reads') }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'incremental',
    unique_key = 'stablecoin_reads_complete_id',
    incremental_predicates = ['dynamic_range', 'partition_key'],
    full_refresh = vars.GLOBAL_STREAMLINE_FR_ENABLED,
    tags = ['streamline','stablecoin_reads','complete','phase_4']
) }}

SELECT
    partition_key,
    contract_address,
    VALUE :"BLOCK_NUMBER" :: NUMBER AS block_number,
    file_name,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address', 'block_number']
    ) }} AS stablecoin_reads_complete_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__stablecoin_reads') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE (MAX(_inserted_timestamp), '1970-01-01' :: TIMESTAMP)
        FROM
            {{ this }})
        {% else %}
            {{ ref('bronze__stablecoin_reads_fr') }}
        {% endif %}

qualify(ROW_NUMBER() over (PARTITION BY stablecoin_reads_complete_id
ORDER BY
    _inserted_timestamp DESC)) = 1