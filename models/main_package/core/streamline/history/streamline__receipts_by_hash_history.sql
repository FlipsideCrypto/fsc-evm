{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = {
            "external_table": "receipts_by_hash",
            "sql_limit": {{MAIN_SL_RECEIPTS_BY_HASH_HISTORY_SQL_LIMIT}},
            "producer_batch_size": {{MAIN_SL_RECEIPTS_BY_HASH_HISTORY_PRODUCER_BATCH_SIZE}},
            "worker_batch_size": {{MAIN_SL_RECEIPTS_BY_HASH_HISTORY_WORKER_BATCH_SIZE}},
            "async_concurrent_requests": {{MAIN_SL_RECEIPTS_BY_HASH_HISTORY_ASYNC_CONCURRENT_REQUESTS}},
            "sql_source" :"{{this.identifier}}"
        }
    ),
    tags = ['streamline_core_history_receipts_by_hash']
) }}

{# Main query starts here #}

WITH 
to_do AS (
    SELECT 
        block_number,
        tx_hash
    FROM {{ ref("core__fact_transactions") }}
    WHERE 
        (block_number IS NOT NULL 
        AND tx_hash IS NOT NULL)

    EXCEPT

    SELECT
        block_number,
        tx_hash
    FROM
        {{ ref('streamline__receipts_by_hash_complete') }}
    WHERE 1=1
),
ready_blocks AS (
    SELECT
        block_number,
        tx_hash
    FROM
        to_do

    {% if MAIN_SL_TESTING_LIMIT is not none %}
        LIMIT {{ MAIN_SL_TESTING_LIMIT }} 
    {% endif %}
)
SELECT
    block_number,
    tx_hash,
    ROUND(
        block_number,
        -3
    ) AS partition_key,
    live.udf_api(
        'POST',
        '{{ node_url }}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json',
            'fsc-quantum-state', 'streamline'
        ),
        OBJECT_CONSTRUCT(
            'id', block_number,
            'jsonrpc', '2.0',
            'method', 'eth_getTransactionReceipt',
            'params', ARRAY_CONSTRUCT(tx_hash)
        ),
        '{{ node_secret_path }}'
    ) AS request
FROM
    ready_blocks

ORDER BY block_number DESC

LIMIT {{ MAIN_SL_RECEIPTS_BY_HASH_HISTORY_SQL_LIMIT }}