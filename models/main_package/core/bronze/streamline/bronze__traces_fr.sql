{{ config (
    materialized = 'view',
    tags = ['bronze_core']
) }}

{{ streamline_external_table_query_fr(
    source_name = 'traces',
    partition_function = bronze_partition_function,
    uses_receipts_by_hash = bronze_uses_receipts_by_hash
) }}