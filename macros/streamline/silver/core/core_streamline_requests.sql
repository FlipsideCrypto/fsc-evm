{% macro core_streamline_block_requests(
    external_table,
    sql_limit,
    producer_batch_size,
    worker_batch_size,
    vault_secret_path,
    exploded_key=false,
    model_tags=["streamline_core_realtime"],
    model_limit=false
) %}

{
    materialized: "view",
    post_hook: fsc_utils.if_data_call_function_v2(
        func = "streamline.udf_bulk_rest_api_v2",
        target = "{{ this.schema }}.{{ this.identifier }}",
        params = { 
            "external_table": "{{ external_table }}",
            "sql_limit": "{{ sql_limit }}",
            "producer_batch_size": "{{ producer_batch_size }}",
            "worker_batch_size": "{{ worker_batch_size }}",
            "sql_source": "{{ this.identifier }}",
            {% if exploded_key %}
            "exploded_key": tojson({{ exploded_key_fields }})
            {% endif %}
        }
    ),
    tags: {{ model_tags}}
}

WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_block_lookback") }}
),
to_do AS (
    SELECT
        block_number
    FROM
        {{ ref("streamline__blocks") }}
    WHERE
        (
            block_number >= (
                SELECT
                    block_number
                FROM
                    last_3_days
            )
        )
        AND block_number IS NOT NULL
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref("streamline__complete_blocks") }}
        b
        INNER JOIN {{ ref("streamline__complete_transactions") }}
        t USING(block_number) -- inner join to ensure that only blocks with both block data and transaction data are excluded
    WHERE
        block_number >= (
            SELECT
                block_number
            FROM
                last_3_days
        )
),
ready_blocks AS (
    SELECT
        block_number
    FROM
        to_do
    UNION
    SELECT
        block_number
    FROM
        (
            SELECT
                block_number
            FROM
                {{ ref("_missing_txs") }}
            UNION
            SELECT
                block_number
            FROM
                {{ ref("_unconfirmed_blocks") }}
        )
)
SELECT
    block_number,
    ROUND(
        block_number,
        -3
    ) AS partition_key,
    {{ target.database }}.live.udf_api(
        'POST',
        '{service}/{Authentication}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),
        OBJECT_CONSTRUCT(
            'id',
            block_number,
            'jsonrpc',
            '2.0',
            'method',
            'eth_getBlockByNumber',
            'params',
            ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), TRUE)),
            --set to TRUE for full txn data
            '{{ vault_secret_path }}'
        ) AS request
        FROM
            ready_blocks
        ORDER BY
            partition_key ASC
        {% if model_limit %}
        LIMIT
            {{ model_limit_num }} 
        {% endif %}
{% endmacro %}
