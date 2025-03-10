
{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"contract_abis",
        "sql_limit" :"100",
        "producer_batch_size" :"1",
        "worker_batch_size" :"1",
        "sql_source" :"{{this.identifier}}" }
    ),
    tags = ['streamline_abis_realtime']
) }}

WITH recent_relevant_contracts AS (

    SELECT
        contract_address,
        total_interaction_count,
        GREATEST(
            max_inserted_timestamp_logs,
            max_inserted_timestamp_traces
        ) max_inserted_timestamp
    FROM
        {{ ref('silver__relevant_contracts') }} C
        LEFT JOIN {{ ref("streamline__complete_contract_abis") }}
        s USING (contract_address)
    WHERE
        s.contract_address IS NULL
        AND total_interaction_count > {{ var('DECODER_ABIS_RELEVANT_CONTRACT_COUNT') }}
        AND max_inserted_timestamp >= DATEADD(DAY, -3, SYSDATE())
    ORDER BY
        total_interaction_count DESC
    LIMIT
        {{ var('DECODER_ABIS_RELEVANT_CONTRACT_LIMIT') }}
), all_contracts AS (
    SELECT
        contract_address
    FROM
        recent_relevant_contracts

{% if is_incremental() %}
UNION
SELECT
    contract_address
FROM
    {{ ref('_retry_abis') }}
{% endif %}
)
SELECT
    contract_address,
    DATE_PART('EPOCH_SECONDS', systimestamp()) :: INT AS partition_key,
    live.udf_api(
        'GET',
        CONCAT(
            '{{ var('DECODER_ABIS_BLOCK_EXPLORER_URL') }}',
            contract_address
            {% if var('DECODER_ABIS_BLOCK_EXPLORER_SECRET_PATH') != '' %}
            ,'&apikey={key}'
            {% endif %}
            {% if var('DECODER_ABIS_BLOCK_EXPLORER_URL_SUFFIX') | default('') != '' %}
            ,'{{ var('DECODER_ABIS_BLOCK_EXPLORER_URL_SUFFIX') }}'
            {% endif %}
        ),
        OBJECT_CONSTRUCT(
            'Content-Type', 'application/json',
            'fsc-quantum-state', 'auto'
        ),
        NULL,
        '{{ var('DECODER_ABIS_BLOCK_EXPLORER_SECRET_PATH') }}'
    ) AS request
FROM
    all_contracts

