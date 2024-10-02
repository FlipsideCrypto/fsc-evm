{{ config (
    materialized = "view",
    tags = ['streamline_core_realtime']
) }}
{# Start by invoking LQ for the last hour of blocks #}
WITH numbered_blocks AS (

    SELECT
        block_number_hex,
        block_number,
        ROW_NUMBER() over (
            ORDER BY
                block_number
        ) AS row_num
    FROM
        (
            SELECT
                *
            FROM
                {{ ref('streamline__blocks') }}
            ORDER BY
                block_number DESC
            LIMIT
                {{ var('BLOCKS_PER_HOUR') }}
        )
), batched_blocks AS (
    SELECT
        block_number_hex,
        block_number,
        100 AS rows_per_batch,
        CEIL(
            row_num / rows_per_batch
        ) AS batch_number,
        MOD(
            row_num - 1,
            rows_per_batch
        ) + 1 AS row_within_batch
    FROM
        numbered_blocks
),
batched_calls AS (
    SELECT
        batch_number,
        ARRAY_AGG(
            utils.udf_json_rpc_call(
                'eth_getBlockByNumber',
                [block_number_hex, false]
            )
        ) AS batch_request
    FROM
        batched_blocks
    GROUP BY
        batch_number
),
rpc_requests AS (
    SELECT
        live.udf_api(
            'POST',
            '{Service}/{Authentication}',
            OBJECT_CONSTRUCT(
                'Content-Type',
                'application/json',
                'fsc-quantum-state',
                'livequery'
            ),
            batch_request,
            'Vault/prod/core/ankr/mainnet'
        ) AS resp
    FROM
        batched_calls
),
blocks AS (
    SELECT
        utils.udf_hex_to_int(
            VALUE :result :number :: STRING
        ) :: INT AS block_number,
        VALUE :result :transactions AS tx_hashes
    FROM
        rpc_requests,
        LATERAL FLATTEN (
            input => resp :data
        )
),
create_receipts_calls AS (
    SELECT
        block_number,
        VALUE :: STRING AS tx_hash,
        utils.udf_json_rpc_call(
            'eth_getTransactionReceipt',
            [tx_hash]
        ) AS receipt_rpc_call
    FROM
        blocks,
        LATERAL FLATTEN (
            input => tx_hashes
        )
),
ready_blocks AS (
    SELECT
        block_number,
        tx_hash,
        receipt_rpc_call
    FROM
        create_receipts_calls
    LIMIT
        10
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
        '{{ var(' api_url ') }}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json',
            'fsc-quantum-state',
            'livequery'
        ),
        receipt_rpc_call,
        '{{ var(' vault_secret_path ') }}'
    ) AS request
FROM
    ready_blocks {{ log("Materialization: " ~ config.get('materialized'), info = True) }}
    {{ log(
        "",
        info = True
    ) }}
    {{ log(
        "=== Streamline Parameters ===",
        info = True
    ) }}

    {%- for key,
        value in params.items() %}
        {{ log(
            key ~ ": " ~ value,
            info = True
        ) }}
    {%- endfor %}
