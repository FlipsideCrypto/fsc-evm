{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
-- depends on: {{ ref('bronze__token_reads') }}
{{ config (
    materialized = 'incremental',
    unique_key = 'complete_token_reads_id',
    cluster_by = 'partition_key',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(complete_token_reads_id, contract_address)",
    incremental_predicates = ['dynamic_range', 'partition_key'],
    full_refresh = vars.GLOBAL_STREAMLINE_FR_ENABLED,
    tags = ['streamline','token_reads','complete','phase_2']
) }}

SELECT
    partition_key,
    contract_address,
    file_name,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address']
    ) }} AS complete_token_reads_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__token_reads') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE (MAX(_inserted_timestamp), '1970-01-01' :: TIMESTAMP)
        FROM
            {{ this }})
        {% else %}
            {{ ref('bronze__token_reads_fr') }}
        {% endif %}

{% if not is_incremental() and vars.GLOBAL_PROJECT_NAME == 'ethereum' %}
UNION
   0 AS partition_key,
   address AS contract_address,
   NULL AS file_name,
   {{ dbt_utils.generate_surrogate_key(
        ['address']
    ) }} AS complete_token_reads_id,
   SYSDATE() AS inserted_timestamp,
   SYSDATE() AS modified_timestamp,
   _inserted_timestamp,
   '{{ invocation_id }}' AS _invocation_id
FROM
    silver.contracts_legacy -- hardcoded for ethereum, to avoid source compiling issues on other chains
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY complete_token_reads_id
ORDER BY
    _inserted_timestamp DESC)) = 1