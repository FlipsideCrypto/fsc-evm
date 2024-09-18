{% macro silver_traces_v1(
        full_reload_start_block,
        full_reload_blocks,
        full_reload_mode = false,
        arb_traces_mode = false,
        use_partition_key = false
    ) %}
    WITH bronze_traces AS (
        SELECT
            block_number,
            {% if use_partition_key %}
                partition_key,
            {% else %}
                _partition_by_block_id AS partition_key,
            {% endif %}

            VALUE :array_index :: INT AS tx_position,
            DATA :result AS full_traces,
            _inserted_timestamp
        FROM

{% if is_incremental() and not full_reload_mode %}
{{ ref('bronze__streamline_traces') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
    AND DATA :result IS NOT NULL {% if arb_traces_mode %}
        AND block_number > 22207817
    {% endif %}

    {% elif is_incremental() and full_reload_mode %}
    {{ ref('bronze__streamline_fr_traces') }}
WHERE
    {% if use_partition_key %}
        partition_key BETWEEN (
            SELECT
                MAX(partition_key) - 100000
            FROM
                {{ this }}
        )
        AND (
            SELECT
                MAX(partition_key) + {{ full_reload_blocks }}
            FROM
                {{ this }}
        )
    {% else %}
        _partition_by_block_id BETWEEN (
            SELECT
                MAX(_partition_by_block_id) - 100000
            FROM
                {{ this }}
        )
        AND (
            SELECT
                MAX(_partition_by_block_id) + {{ full_reload_blocks }}
            FROM
                {{ this }}
        )
    {% endif %}

    {% if arb_traces_mode %}
        AND block_number > 22207817
    {% endif %}
{% else %}
    {{ ref('bronze__streamline_fr_traces') }}
WHERE
    {% if use_partition_key %}
        partition_key <= {{ full_reload_start_block }}
    {% else %}
        _partition_by_block_id <= {{ full_reload_start_block }}
    {% endif %}

    {% if arb_traces_mode %}
        AND block_number > 22207817
    {% endif %}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number, tx_position
ORDER BY
    _inserted_timestamp DESC)) = 1
),
flatten_traces AS (
    SELECT
        block_number,
        tx_position,
        partition_key,
        IFF(
            path IN (
                'result',
                'result.value',
                'result.type',
                'result.to',
                'result.input',
                'result.gasUsed',
                'result.gas',
                'result.from',
                'result.output',
                'result.error',
                'result.revertReason',
                'result.time',
                'gasUsed',
                'gas',
                'type',
                'to',
                'from',
                'value',
                'input',
                'error',
                'output',
                'time',
                'revertReason' {% if arb_traces_mode %},
                    'afterEVMTransfers',
                    'beforeEVMTransfers',
                    'result.afterEVMTransfers',
                    'result.beforeEVMTransfers'
                {% endif %}
            ),
            'ORIGIN',
            REGEXP_REPLACE(REGEXP_REPLACE(path, '[^0-9]+', '_'), '^_|_$', '')
        ) AS trace_address,
        _inserted_timestamp,
        OBJECT_AGG(
            key,
            VALUE
        ) AS trace_json,
        CASE
            WHEN trace_address = 'ORIGIN' THEN NULL
            WHEN POSITION(
                '_' IN trace_address
            ) = 0 THEN 'ORIGIN'
            ELSE REGEXP_REPLACE(
                trace_address,
                '_[0-9]+$',
                '',
                1,
                1
            )
        END AS parent_trace_address,
        SPLIT(
            trace_address,
            '_'
        ) AS trace_address_array
    FROM
        bronze_traces txs,
        TABLE(
            FLATTEN(
                input => PARSE_JSON(
                    txs.full_traces
                ),
                recursive => TRUE
            )
        ) f
    WHERE
        f.index IS NULL
        AND f.key != 'calls'
        AND f.path != 'result' {% if arb_traces_mode %}
            AND f.path NOT LIKE 'afterEVMTransfers[%'
            AND f.path NOT LIKE 'beforeEVMTransfers[%'
        {% endif %}
    GROUP BY
        block_number,
        tx_position,
        partition_key,
        trace_address,
        _inserted_timestamp
)
SELECT
    block_number,
    tx_position,
    trace_address,
    parent_trace_address,
    trace_address_array,
    trace_json,
    partition_key,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number', 'tx_position', 'trace_address']
    ) }} AS traces_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    flatten_traces qualify(ROW_NUMBER() over(PARTITION BY traces_id
ORDER BY
    _inserted_timestamp DESC)) = 1
{% endmacro %}