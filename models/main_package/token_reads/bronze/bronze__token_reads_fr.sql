{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
-- depends_on: {{ ref('bronze_api__token_reads') }}

{{ config (
    materialized = 'view',
    tags = ['bronze','token_reads','phase_2']
) }}

SELECT
    {# partition_key,
    contract_address,
    VALUE,
    metadata,
    DATA,
    file_name,
    _inserted_timestamp #}
FROM
    {{ ref('bronze__token_reads_fr_v2') }}

{% if vars.MAIN_SL_TOKEN_READS_BRONZE_TABLE_ENABLED %}
UNION ALL
SELECT
    ROUND(block_number,-3) AS partition_key,
    contract_address,
    block_number,
    function_sig,
    read_result,
    _inserted_timestamp
FROM
    {{ ref('bronze_api__token_reads') }}
{% endif %}
