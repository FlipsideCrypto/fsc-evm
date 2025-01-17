{% macro streamline_decoded_traces_requests(
        start,
        stop,
        model_type,
        testing_limit
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
    concat_ws(
            '-',
            t.block_number,
            t.tx_position,
            concat(t.type,'_',t.trace_address)
    ) as _call_id,
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
    {{ ref("core__fact_traces") }}
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
                {{ ref("streamline__decoded_traces_complete") }}
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
                        {{ ref("streamline__decoded_traces_complete") }}
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
    {% if testing_limit is not none %}
        LIMIT {{ testing_limit }} 
    {% endif %}
{% endmacro %}
