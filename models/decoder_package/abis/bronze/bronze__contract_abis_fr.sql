{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
-- depends_on: {{ ref('bronze_api__contract_abis') }}

{{ config (
    materialized = 'view',
    tags = ['bronze','abis','phase_2']
) }}

SELECT
    partition_key,
    contract_address,
    VALUE,
    metadata,
    DATA,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__contract_abis_fr_v2') }}

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
