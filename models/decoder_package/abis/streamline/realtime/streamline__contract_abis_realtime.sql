{% set block_explorer_abi_url_suffix = var('BLOCK_EXPLORER_ABI_URL_SUFFIX', '') %}
{% set block_explorer_vault_path = var('BLOCK_EXPLORER_ABI_API_KEY_PATH', '') %}

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
        AND total_interaction_count > {{ var('BLOCK_EXPLORER_ABI_INTERACTION_LIMIT') }}
        AND max_inserted_timestamp >= DATEADD(DAY, -3, SYSDATE())
    ORDER BY
        total_interaction_count DESC
    LIMIT
        {{ var('BLOCK_EXPLORER_ABI_LIMIT') }}
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
            '{{ var('BLOCK_EXPLORER_ABI_URL') }}',
            contract_address
            {% if block_explorer_vault_path != '' %}
            ,'&apikey={key}'
            {% endif %}
            {% if block_explorer_abi_url_suffix != '' %}
            ,'{{ block_explorer_abi_url_suffix }}'
            {% endif %}
        ),
        { 'User-Agent': 'FlipsideStreamline' },
        NULL,
        '{{ var('BLOCK_EXPLORER_ABI_API_KEY_PATH') }}'
    ) AS request
FROM
    all_contracts
