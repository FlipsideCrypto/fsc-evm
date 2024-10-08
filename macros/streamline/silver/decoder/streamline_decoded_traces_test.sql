{% macro streamline_decoded_traces_requests_test(
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
            END AS parent_trace_grouping,
            IFF(REGEXP_REPLACE(trace_address, '.$', '') = '', 'ORIGIN', REGEXP_REPLACE(trace_address, '.$', '')) AS parent_grouping,
            input,
            output
        FROM
            {{ ref("silver__traces") }}
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
                        parent_trace_grouping AS parent_grouping,
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
                        to_address AS effective_implementation,
                        parent_grouping AS parent_trace_grouping,
                        input
                    FROM
                        raw_traces
                        INNER JOIN PARENT USING (
                            tx_hash,
                            parent_grouping,
                            input
                        )
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
                        parent_trace_grouping,
                        parent_grouping,
                        input,
                        output,
                        COALESCE(
                            effective_implementation,
                            to_address
                        ) AS effective_contract_address
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
                t.effective_contract_address,
                f.abi AS abi,
                f.function_name,
                t.input,
                COALESCE(
                    t.output,
                    '0x'
                ) AS output
            FROM
                final_traces t
                INNER JOIN {{ ref("silver__flat_function_abis") }}
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
