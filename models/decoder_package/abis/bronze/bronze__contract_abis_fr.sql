{{ config (
    materialized = 'view'
) }}

WITH externals AS (
    {{ fsc_evm.streamline_external_table_query_fr(
        source_name = 'contract_abis',
        source_version = '',
        partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER)",
        partition_join_key = 'partition_key'
    ) }}
)
SELECT
    partition_key,
    COALESCE(
        VALUE :"CONTRACT_ADDRESS",
        VALUE :"contract_address"
    ) :: STRING AS contract_address,
    VALUE AS abi_data,
    metadata,
    DATA,
    file_name,
    _inserted_timestamp
FROM
    externals

    {% if var(
        'DECODER_ABIS_BRONZE_API_TABLE_ENABLED',
        false
    ) %}
UNION ALL
SELECT
    1 AS partition_key,
    contract_address,
    abi_data,
    NULL AS metadata,
    abi_data :data AS DATA,
    NULL AS file_name,
    _inserted_timestamp _inserted_timestamp
FROM
    {{ ref('bronze_api__contract_abis') }}
{% endif %}
