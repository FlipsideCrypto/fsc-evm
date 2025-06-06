{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('bronze_api__token_reads') }}

{{ config (
    materialized = 'view',
    tags = ['bronze','token_reads','phase_2']
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
    {{ ref('bronze__token_reads_fr_v2') }}

{% if vars.MAIN_SL_TOKEN_READS_BRONZE_TABLE_ENABLED %}
UNION ALL
SELECT
    ROUND(block_number,-3) AS partition_key,
    contract_address,
    OBJECT_CONSTRUCT_KEEP_NULL(
        'CONTRACT_ADDRESS', contract_address,
        'FUNCTION_SIG', function_sig,
        'INPUT', RPAD(function_sig,64,'0'),
        'LATEST_BLOCK', block_number,
        'data', OBJECT_CONSTRUCT_KEEP_NULL(
            'id', concat_ws(
                '-',
                contract_address,
                RPAD(function_sig,64,'0'),
                block_number
            ),
            'jsonrpc', '2.0',
            'result', read_result
        ),
        'metadata', NULL,
        'partition_key', partition_key
    ) AS VALUE,
    NULL AS metadata,
    VALUE :data AS DATA,
    NULL AS file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze_api__token_reads') }}
{% endif %}
