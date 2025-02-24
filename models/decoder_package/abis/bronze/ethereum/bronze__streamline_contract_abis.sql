{{ config (
    materialized = 'view'
) }}
{{ fsc_evm.streamline_external_table_query(
    source_name = 'contract_abis',
    source_version = 'v2',
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER)"
) }}
