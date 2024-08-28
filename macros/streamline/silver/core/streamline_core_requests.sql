{% macro streamline_core_chainhead(
        vault_secret_path
    ) %}
SELECT
    live.udf_api(
        'POST',
        '{service}/{Authentication}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json',
            'fsc-quantum-state',
            'livequery'
        ),
        OBJECT_CONSTRUCT(
            'id',
            0,
            'jsonrpc',
            '2.0',
            'method',
            'eth_blockNumber',
            'params',
            []
        ),
        '{{ vault_secret_path }}'
    ) AS resp,
    utils.udf_hex_to_int(
        resp :data :result :: STRING
    ) AS block_number
{% endmacro %}

{% macro streamline_core_requests(
    vault_secret_path,
    query_limit,
    quantum_state,
    realtime=false,
    history=false,
    blocks_transactions=false,
    receipts=false,
    traces=false,
    confirmed_blocks=false
) %}

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
    {% elif traces %}
    SELECT
        block_number
    FROM
        {{ ref("_missing_traces") }}
    {% endif %}
    )
)
{% endif %}
SELECT
    block_number,
    ROUND(
        block_number,
        -3
    ) AS partition_key,
    live.udf_api(
        'POST',
        '{service}/{Authentication}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        {% if quantum_state == 'streamline' %}
            ,'fsc-quantum-state',
            'streamline'
        ),
        {% elif quantum_state == 'livequery' %}
            ,'fsc-quantum-state',
            'livequery'
        ),
        {% else %}
        ),
        {% endif %}
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
        {% if query_limit %}
        LIMIT
            {{ query_limit }} 
        {% endif %}
{% endmacro %}
