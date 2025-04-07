{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
-- depends on: {{ ref('bronze__contract_abis') }}
{{ config (
    materialized = 'incremental',
    unique_key = 'complete_contract_abis_id',
    cluster_by = 'partition_key',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(complete_contract_abis_id, contract_address)",
    incremental_predicates = ['dynamic_range', 'partition_key'],
    full_refresh = vars.GLOBAL_STREAMLINE_FR_ENABLED,
    tags = ['silver','streamline','abis','complete']
) }}

SELECT
    partition_key,
    contract_address,
    file_name,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address']
    ) }} AS complete_contract_abis_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__contract_abis') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE (MAX(_inserted_timestamp), '1970-01-01' :: TIMESTAMP)
        FROM
            {{ this }})
        {% else %}
            {{ ref('bronze__contract_abis_fr') }}
        {% endif %}

        qualify(ROW_NUMBER() over (PARTITION BY complete_contract_abis_id
        ORDER BY
            _inserted_timestamp DESC)) = 1
