{% macro streamline_decoded_logs_requests(
        start,
        stop,
        model_type,
        query_limit
    ) %}
    WITH look_back AS ({% if model_type == 'realtime' %}
    SELECT
        block_number
    FROM
        {{ ref("_24_hour_lookback") }}

        {% elif model_type == 'history' %}
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
    {{ ref("core__fact_event_logs") }}
    l
    INNER JOIN {{ ref("silver__complete_event_abis") }} A
    ON A.parent_contract_address = l.contract_address
    AND A.event_signature = l.topics [0] :: STRING
    AND l.block_number BETWEEN A.start_block
    AND A.end_block
WHERE
    {% if model_type == 'realtime' %}
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
                AND _inserted_timestamp >= DATEADD('day', -2, CURRENT_DATE())) {% elif model_type == 'history' %}
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

            {% if query_limit %}
            LIMIT
                {{ query_limit }}
            {% endif %}
{% endmacro %}

{% macro streamline_decoded_traces_requests(
        start,
        stop,
        model_type,
        query_limit
    ) %}
    WITH look_back AS (
        SELECT
            block_number
        FROM
            {{ ref("_24_hour_lookback") }}
    ),
    raw_traces AS (
        SELECT
            block_number,
            tx_hash,
            trace_index,
            from_address,
            to_address,
            TYPE,
            REGEXP_REPLACE(
                identifier,
                '[A-Z]+_',
                ''
            ) AS trace_address,
            sub_traces,
            CASE
                WHEN sub_traces > 0
                AND trace_address = 'ORIGIN' THEN 'ORIGIN'
                WHEN sub_traces > 0
                AND trace_address != 'ORIGIN' THEN trace_address || '_'
                ELSE NULL
            END AS parent_of,
            -- this adds an underscore
            IFF(REGEXP_REPLACE(trace_address, '.$', '') = '', 'ORIGIN', REGEXP_REPLACE(trace_address, '.$', '')) AS child_of,
            -- removes the last character
            input,
            output,
            _call_id
        FROM
            {{ ref("silver__traces") }}
            -- have to make this silver temporarily
            t
        WHERE
            {% if model_type == 'realtime' %}
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
                        AND _inserted_timestamp >= DATEADD('day', -2, CURRENT_DATE())) {% elif model_type == 'history' %}
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
                ),
                PARENT AS (
                    -- first takes trace calls where there are subtraces. These are the parent calls
                    SELECT
                        tx_hash,
                        parent_of AS child_of,
                        input
                    FROM
                        raw_traces
                    WHERE
                        sub_traces > 0
                ),
                effective_contract AS (
                    -- finds the effective implementation address for the parent trace
                    SELECT
                        tx_hash,
                        TYPE AS child_type,
                        to_address AS child_to_address,
                        -- effective implementation
                        child_of AS parent_of,
                        input
                    FROM
                        raw_traces
                        INNER JOIN PARENT USING (
                            tx_hash,
                            parent_grouping,
                            input
                        ) -- where type = 'DELEGATECALL'
                ),
                final_traces AS (
                    SELECT
                        block_number,
                        tx_hash,
                        trace_index,
                        from_address,
                        to_address,
                        TYPE,
                        trace_address,
                        sub_traces,
                        parent_of,
                        child_of,
                        input,
                        output,
                        child_type,
                        child_to_address,
                        IFF(
                            child_type = 'DELEGATECALL'
                            AND child_to_address IS NOT NULL,
                            child_to_address,
                            to_address
                        ) AS effective_contract_address,
                        _call_id
                    FROM
                        raw_traces
                        LEFT JOIN effective_contract USING (
                            tx_hash,
                            parent_trace_grouping,
                            input
                        )
                )
            SELECT
                t.block_number,
                t.tx_hash,
                t.trace_index,
                _call_id,
                f.abi AS abi,
                f.function_name,
                t.effective_contract_address AS abi_address,
                t.input,
                COALESCE(
                    t.output,
                    '0x'
                ) AS output
            FROM
                final_traces t
                LEFT JOIN {{ ref("silver__flat_function_abis") }}
                f
                ON t.effective_contract_address = f.contract_address
                AND LEFT(
                    t.input,
                    10
                ) = LEFT(
                    f.function_signature,
                    10
                ) {% if query_limit %}
                LIMIT
                    {{ query_limit }}
                {% endif %}
{% endmacro %}
