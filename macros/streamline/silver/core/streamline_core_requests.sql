{% macro streamline_core_chainhead(
    quantum_state,
    vault_secret_path,
    api_url='{Service}/{Authentication}'
) %}
SELECT
    live.udf_api(
        'POST',
        '{{ api_url }}',
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
    model_type,
    model,
    quantum_state,
    vault_secret_path,
    query_limit,
    api_url='{Service}/{Authentication}',
    order_by_clause='ORDER BY partition_key ASC',
    new_build=false,
    testing_limit=none
) %}

WITH 
{% if not new_build %}
    last_3_days AS (
        SELECT block_number
        FROM {{ ref("_block_lookback") }}
    ),

    {% if model == 'confirmed_blocks' %}
        look_back AS (
            SELECT block_number
            FROM {{ ref("_max_block_by_hour") }}
            QUALIFY ROW_NUMBER() OVER (ORDER BY block_number DESC) = 6
        ),
    {% endif %}
{% endif %}

to_do AS (
    SELECT block_number
    FROM {{ ref("streamline__blocks") }}
    WHERE 
        block_number IS NOT NULL
        {% if not new_build %}
        AND block_number
            {% if model_type == 'realtime' %}>={% elif model_type == 'history' %}<={% endif %}
            (SELECT block_number FROM last_3_days)
        {% endif %}
        {% if model == 'confirmed_blocks' and not new_build %}
            AND block_number <= (SELECT block_number FROM look_back)
        {% endif %}

    EXCEPT

    SELECT block_number
    FROM
        {% if model == 'blocks_transactions' %}
            {{ ref("streamline__complete_blocks") }} b
            INNER JOIN {{ ref("streamline__complete_transactions") }} t USING(block_number)
        {% elif model == 'receipts' %}
            {{ ref("streamline__complete_receipts") }}
        {% elif model == 'traces' %}
            {{ ref("streamline__complete_traces") }}
        {% elif model == 'confirmed_blocks' %}
            {{ ref("streamline__complete_confirmed_blocks") }}
        {% endif %}
    WHERE 1=1
        {% if not new_build %}
            AND block_number
            {% if model_type == 'realtime' %}>={% elif model_type == 'history' %}<={% endif %}
            (SELECT block_number FROM last_3_days)
            {% if model == 'confirmed_blocks' %}
                AND block_number IS NOT NULL
                AND block_number <= (SELECT block_number FROM look_back)
                AND _inserted_timestamp >= DATEADD('day', -4, SYSDATE())
                AND block_number >= (SELECT block_number FROM last_3_days)
            {% endif %}
        {% endif %}
)

{% if model != 'confirmed_blocks' %}
    ,ready_blocks AS (
        SELECT block_number
        FROM to_do

        {% if not new_build %}
            UNION
            SELECT block_number
            FROM (
                SELECT block_number
                FROM {{ ref("_unconfirmed_blocks") }}

                UNION

                {% if model == 'blocks_transactions' %}
                    SELECT block_number
                    FROM {{ ref("_missing_txs") }}
                {% elif model == 'receipts' %}
                    SELECT block_number
                    FROM {{ ref("_missing_txs") }}
                    UNION
                    SELECT block_number
                    FROM {{ ref("_missing_receipts") }}
                {% elif model == 'traces' %}
                    SELECT block_number
                    FROM {{ ref("_missing_traces") }}
                {% endif %}
            )
        {% endif %}
        {% if testing_limit is not none %}
        LIMIT {{ testing_limit }}
        {% endif %}
    )
{% endif %}

SELECT
    block_number,
    ROUND(block_number, -3) AS partition_key,
    live.udf_api(
        'POST',
        '{{ api_url }}',
        OBJECT_CONSTRUCT(
            'Content-Type', 'application/json'
            {% if quantum_state == 'streamline' %}
                ,'fsc-quantum-state', 'streamline'
            {% elif quantum_state == 'livequery' %}
                ,'fsc-quantum-state', 'livequery'
            {% endif %}
        ),
        OBJECT_CONSTRUCT(
            'id', block_number,
            'jsonrpc', '2.0',
            'method',
            {% if model == 'blocks_transactions' %}
                'eth_getBlockByNumber',
                'params', ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), TRUE)
            {% elif model == 'receipts' %}
                'eth_getBlockReceipts',
                'params', ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number))
            {% elif model == 'traces' %}
                'debug_traceBlockByNumber',
                'params', ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), OBJECT_CONSTRUCT('tracer', 'callTracer', 'timeout', '30s'))
            {% elif model == 'confirmed_blocks' %}
                'eth_getBlockByNumber',
                'params', ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), FALSE)
            {% endif %}
        ),
        '{{ vault_secret_path }}'
    ) AS request
FROM
    {% if model != 'confirmed_blocks' %}
        ready_blocks
    {% else %}
        to_do
    {% endif %}
{{ order_by_clause }}
{% if query_limit %}
LIMIT {{ query_limit }} 
{% endif %}

{% endmacro %}