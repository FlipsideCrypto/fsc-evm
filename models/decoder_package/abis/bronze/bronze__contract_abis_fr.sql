{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze_abis']
) }}

{# Main query starts here #}
WITH streamline_abis AS (
    {{ streamline_external_table_query_fr(
        source_name = 'contract_abis',
        contract_address = true
    ) }}
)
SELECT
    partition_key,
    contract_address,
    VALUE,
    metadata,
    DATA,
    file_name,
    _inserted_timestamp
FROM
    streamline_abis

{% if vars.DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED %}
UNION ALL
SELECT
    1 AS partition_key,
    contract_address,
    abi_data AS VALUE,
    NULL AS metadata,
    abi_data :data AS DATA,
    NULL AS file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze_api__contract_abis') }}
{% endif %}
