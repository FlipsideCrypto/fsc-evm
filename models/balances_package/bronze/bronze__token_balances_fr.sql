{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = 'view',
    tags = ['bronze_balances']
) }}

SELECT
    partition_key,
    block_number,
    VALUE :"ADDRESS" :: STRING AS address,
    VALUE :"CONTRACT_ADDRESS" :: STRING AS contract_address,
    block_timestamp,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__token_balances_fr_v2') }}
UNION ALL
SELECT
    _partition_by_block_id AS partition_key,
    block_number,
    address,
    contract_address,
    block_timestamp,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__token_balances_fr_v1') }}
