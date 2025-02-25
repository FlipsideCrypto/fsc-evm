{{ config (
    materialized = 'view',
    tags = ['bronze_receipts']
) }}

{{ streamline_external_table_query(
    source_name = 'receipts',
    partition_function = bronze_partition_function,
    uses_receipts_by_hash = bronze_uses_receipts_by_hash
) }}