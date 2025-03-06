{{ config (
    materialized = 'view',
    tags = ['bronze_abis']
) }}

WITH externals AS (
    {{ fsc_evm.streamline_external_table_query(
        source_name = 'contract_abis',
        source_version = '',
        partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER)"
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
