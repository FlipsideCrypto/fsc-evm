{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ 
            "external_table" :"contract_abis",
            "sql_limit" : vars.DECODER_SL_CONTRACT_ABIS_REALTIME_SQL_LIMIT,
            "producer_batch_size" : vars.DECODER_SL_CONTRACT_ABIS_REALTIME_PRODUCER_BATCH_SIZE,
            "worker_batch_size" : vars.DECODER_SL_CONTRACT_ABIS_REALTIME_WORKER_BATCH_SIZE,
            "sql_source" : 'contract_abis_realtime'}
    ),
    tags = ['streamline','abis','realtime','phase_2']
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
        AND total_interaction_count > {{ vars.DECODER_SL_CONTRACT_ABIS_INTERACTION_COUNT }}
        AND max_inserted_timestamp >= DATEADD(DAY, -3, SYSDATE())
    ORDER BY
        total_interaction_count DESC
    LIMIT
        {{ vars.DECODER_SL_CONTRACT_ABIS_REALTIME_SQL_LIMIT }}
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
            '{{ vars.DECODER_SL_CONTRACT_ABIS_EXPLORER_URL }}',
            contract_address
            {% if vars.DECODER_SL_CONTRACT_ABIS_EXPLORER_VAULT_PATH != '' %}
            ,'&apikey={key}'
            {% endif %}
            {% if vars.DECODER_SL_CONTRACT_ABIS_EXPLORER_URL_SUFFIX != '' %}
            ,'{{ vars.DECODER_SL_CONTRACT_ABIS_EXPLORER_URL_SUFFIX }}'
            {% endif %}
        ),
        OBJECT_CONSTRUCT(
            'Content-Type', 'application/json',
            'fsc-quantum-state', 'streamline'
        ),
        NULL,
        '{{ vars.DECODER_SL_CONTRACT_ABIS_EXPLORER_VAULT_PATH }}'
    ) AS request
FROM
    all_contracts

