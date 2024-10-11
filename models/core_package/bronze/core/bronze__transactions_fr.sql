{{ config (
    materialized = 'view'
) }}

SELECT
    partition_key,
    block_number,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__transactions_fr_v2') }}
UNION ALL
SELECT
    _partition_by_block_id AS partition_key,
    block_number,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
   {{ ref('bronze__transactions_fr_v1') }}
