{% macro streamline_core_requests(
    external_table,
    sql_limit,
    producer_batch_size,
    worker_batch_size,
    vault_secret_path,
    model_limit_num,
    exploded_key_fields,
    exploded_key=false,
    model_limit=false,
    blocks_transactions=false,
    receipts=false,
    traces=false,
    confirmed_blocks=false,
    realtime=false,
    history=false,
    model_tags=[]
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
    tags: {{ model_tags }}
}

WITH last_3_days AS (
    SELECT
        block_number
    FROM
        {{ ref("_block_lookback") }}
),
{% if confirmed_blocks %}
look_back AS (
    SELECT
        block_number
    FROM
        {{ ref("_max_block_by_hour") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_number DESC
        ) = 6
),
{% endif %}
to_do AS (
    SELECT
        block_number
    FROM
        {{ ref("streamline__blocks") }}
    WHERE
        block_number IS NOT NULL
        AND block_number
        {% if realtime %}
        >= 
        {% elif history %}
        <= 
        {% endif %}
        (
                SELECT
                    block_number
                FROM
                    last_3_days
            )
        {% if confirmed_blocks %}
        AND block_number <= (
            SELECT
                block_number
            FROM
                look_back
        )
    {% endif %}
    EXCEPT
    SELECT
        block_number
    FROM
    {% if blocks_transactions %}
        {{ ref("streamline__complete_blocks") }}
        b
        INNER JOIN {{ ref("streamline__complete_transactions") }}
        t USING(block_number) -- inner join to ensure that only blocks with both block data and transaction data are excluded
    {% elif receipts %}
        {{ ref("streamline__complete_receipts") }}
    {% elif traces %}
        {{ ref("streamline__complete_traces") }}
    {% elif confirmed_blocks%}
        {{ ref("streamline__complete_confirmed_blocks") }}
    {% endif %}
    WHERE
        block_number
        {% if realtime %}
        >= 
        {% elif history %}
        <= 
        {% endif %}
        (
            SELECT
                block_number
            FROM
                last_3_days
        )
    {% if confirmed_blocks %}
        AND block_number IS NOT NULL
        AND block_number <= (
            SELECT
                block_number
            FROM
                look_back
        )
        AND _inserted_timestamp >= DATEADD(
            'day',
            -4,
            SYSDATE()
        )
        AND block_number >= (
            SELECT
                block_number
            FROM
                last_3_days
        )
    {% endif %}
)
{% if not confirmed_blocks%}
,ready_blocks AS (
    SELECT
        block_number
    FROM
        to_do
    UNION
    SELECT
        block_number
    FROM (
        SELECT
            block_number
        FROM
            {{ ref("_unconfirmed_blocks") }}
        UNION
    {% if blocks_transactions %}
    SELECT
        block_number
    FROM
        {{ ref("_missing_txs") }}
        )
    {% elif receipts %}
    SELECT
        block_number
    FROM
        {{ ref("_missing_txs") }}
    UNION
    SELECT
        block_number
    FROM
        {{ ref("_missing_receipts") }}
    )
    {% elif traces %}
    SELECT
        block_number
    FROM
        {{ ref("_missing_traces") }}
    )
    {% endif %}
)
{% endif %}
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
        {% if blocks_transactions %}
            'eth_getBlockByNumber',
            'params',
            ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), TRUE)),
            --set to TRUE for full txn data
        {% elif receipts %}
            'eth_getBlockReceipts',
            'params',
            ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number))),
        {% elif traces %}
            'debug_traceBlockByNumber',
            'params',
            ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), OBJECT_CONSTRUCT('tracer', 'callTracer', 'timeout', '30s'))),
        {% elif confirmed_blocks %}
            'eth_getBlockByNumber',
            'params',
            ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), FALSE)),
        {% endif %}
        '{{ vault_secret_path }}'
    ) AS request
        FROM
            {% if not confirmed_blocks %}
                ready_blocks
            {% else %}
                to_do
            {% endif %}
        ORDER BY
            partition_key ASC
        {% if model_limit %}
        LIMIT
            {{ model_limit_num }} 
        {% endif %}
{% endmacro %}
