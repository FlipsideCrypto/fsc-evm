{{ config (
    materialized = 'view'
) }}
{{ fsc_evm.streamline_external_table_query_fr(
    source_name = 'contract_abis',
    source_version = '',
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER)",
    partition_join_key = "_partition_by_block_id",
    block_number = false
) }}
