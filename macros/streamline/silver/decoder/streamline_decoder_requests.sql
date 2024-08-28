{% macro streamline_decoded_logs_requests(
        start,
        stop,
        realtime=false,
        history=false
    ) %}
    WITH look_back AS ({% if realtime %}
    SELECT
        block_number
    FROM
        {{ ref("_24_hour_lookback") }}

        {% elif history %}
    SELECT
        block_number
    FROM
        {{ ref("_max_block_by_date") }}
        qualify ROW_NUMBER() over (
    ORDER BY
        block_number DESC) = 1
    {% endif %})
SELECT
    l.block_number,
    l._log_id,
    A.abi AS abi,
    OBJECT_CONSTRUCT(
        'topics',
        l.topics,
        'data',
        l.data,
        'address',
        l.contract_address
    ) AS DATA
FROM
    {{ ref("silver__logs") }}
    l
    INNER JOIN {{ ref("silver__complete_event_abis") }} A
    ON A.parent_contract_address = l.contract_address
    AND A.event_signature = l.topics [0] :: STRING
    AND l.block_number BETWEEN A.start_block
    AND A.end_block
WHERE
    {% if realtime %}
        (
            l.block_number >= (
                SELECT
                    block_number
                FROM
                    look_back
            )
        )
        AND l.block_number IS NOT NULL
        AND l.block_timestamp >= DATEADD('day', -2, CURRENT_DATE())
        AND _log_id NOT IN (
            SELECT
                _log_id
            FROM
                {{ ref("streamline__complete_decoded_logs") }}
            WHERE
                block_number >= (
                    SELECT
                        block_number
                    FROM
                        look_back
                )
                AND _inserted_timestamp >= DATEADD('day', -2, CURRENT_DATE())) {% elif history %}
                (
                    l.block_number BETWEEN {{ start }}
                    AND {{ stop }}
                )
                AND l.block_number <= (
                    SELECT
                        block_number
                    FROM
                        look_back
                )
                AND _log_id NOT IN (
                    SELECT
                        _log_id
                    FROM
                        {{ ref("streamline__complete_decoded_logs") }}
                    WHERE
                        (
                            block_number BETWEEN {{ start }}
                            AND {{ stop }}
                        )
                        AND block_number <= (
                            SELECT
                                block_number
                            FROM
                                look_back
                        )
                )
            {% endif %}
{% endmacro %}

{% macro streamline_decoded_traces_requests(
        start,
        stop,
        realtime=false,
        history=false
    ) %}
    WITH look_back AS (
        SELECT
            block_number
        FROM
            {{ ref("_24_hour_lookback") }}
    )
SELECT
    t.block_number,
    t.tx_hash,
    t.trace_index,
    _call_id,
    A.abi AS abi,
    A.function_name AS function_name,
    CASE
        WHEN TYPE = 'DELEGATECALL' THEN from_address
        ELSE to_address
    END AS abi_address,
    t.input AS input,
    COALESCE(
        t.output,
        '0x'
    ) AS output
FROM
    {{ ref("silver__traces") }}
    t
    INNER JOIN {{ ref("silver__complete_function_abis") }} A
    ON A.parent_contract_address = abi_address
    AND LEFT(
        t.input,
        10
    ) = LEFT(
        A.function_signature,
        10
    )
    AND t.block_number BETWEEN A.start_block
    AND A.end_block
WHERE
    {% if realtime %}
        t.block_number >= (
            SELECT
                block_number
            FROM
                look_back
        )
        AND t.block_number IS NOT NULL
        AND t.block_timestamp >= DATEADD('day', -2, CURRENT_DATE())
        AND _call_id NOT IN (
            SELECT
                _call_id
            FROM
                {{ ref("streamline__complete_decoded_traces") }}
            WHERE
                block_number >= (
                    SELECT
                        block_number
                    FROM
                        look_back
                )
                AND _inserted_timestamp >= DATEADD('day', -2, CURRENT_DATE())) {% elif history %}
                (
                    t.block_number BETWEEN {{ start }}
                    AND {{ stop }}
                )
                AND t.block_number < (
                    SELECT
                        block_number
                    FROM
                        look_back
                )
                AND t.block_number IS NOT NULL
                AND _call_id NOT IN (
                    SELECT
                        _call_id
                    FROM
                        {{ ref("streamline__complete_decoded_traces") }}
                    WHERE
                        (
                            block_number BETWEEN {{ start }}
                            AND {{ stop }}
                        )
                        AND block_number < (
                            SELECT
                                block_number
                            FROM
                                look_back
                        )
                )
            {% endif %}
{% endmacro %}
