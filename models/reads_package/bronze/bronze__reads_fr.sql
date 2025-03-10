{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = 'view',
    tags = ['bronze_reads']
) }}

SELECT
    partition_key,
    _partition_by_function_signature,
    block_number,
    VALUE :"CONTRACT_ADDRESS" :: STRING AS contract_address,
    VALUE :"FUNCTION_SIGNATURE" :: STRING AS function_signature,
    VALUE :"FUNCTION_INPUT" :: STRING AS function_input,
    VALUE :"CALL_NAME" :: STRING AS call_name,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__reads_fr_v2') }}
UNION ALL
SELECT
    _partition_by_modified_date AS partition_key,
    _partition_by_function_signature,
    block_number,
    contract_address,
    function_signature,
    function_input,
    call_name,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__reads_fr_v1') }}
