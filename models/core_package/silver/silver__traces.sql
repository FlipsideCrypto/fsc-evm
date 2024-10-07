{{ set full_reload_start_block = var('TRACES_FULL_RELOAD_START_BLOCK', 0)}}
{{ set full_reload_blocks = var('TRACES_FULL_RELOAD_BLOCKS', 1000000)}}
{{ set full_reload_mode = var('SILVER_TRACES_FULL_RELOAD_MODE', false)}}
{{ set arb_traces_mode = var('ARB_TRACES_MODE', false)}}
{{ set sei_traces_mode = var('EI_TRACES_MODE', false)}}
{{ set kaia_traces_mode = var('KAIA_TRACES_MODE', false)}}
{{ set use_partition_key = var('USE_PARTITION_KEY', false)}}
{{ set schema_name = var('TRACES_SCHEMA_NAME', 'bronze')}}

-- depends_on: {{ ref('bronze__traces') }}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    full_refresh = false,
    tags = ['realtime']
) }}

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
            {% if sei_traces_mode %}
                DATA :txHash :: STRING AS tx_hash,
            {% endif %}
            _inserted_timestamp
        FROM

{% if is_incremental() and not full_reload_mode %}
{{ ref(schema_name ~ '__traces') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    ) AND DATA :result IS NOT NULL 
    {% if arb_traces_mode %}
        AND block_number > 22207817
    {% endif %}

    {% elif is_incremental() and full_reload_mode %}
    {{ ref(schema_name ~ '__traces_fr') }}
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
    {{ ref(schema_name ~ '__traces_fr') }}
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
        {% if sei_traces_mode %}
            tx_hash,
        {% else %}
            tx_position,
        {% endif %}
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
                'revertReason' 
                {% if arb_traces_mode %},
                    'afterEVMTransfers',
                    'beforeEVMTransfers',
                    'result.afterEVMTransfers',
                    'result.beforeEVMTransfers'
                {% endif %}
                {% if kaia_traces_mode %},
                    'reverted',
                    'result.reverted'
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
        AND f.path != 'result' 
        {% if arb_traces_mode %}
            AND f.path NOT LIKE 'afterEVMTransfers[%'
            AND f.path NOT LIKE 'beforeEVMTransfers[%'
        {% endif %}
        {% if kaia_traces_mode %}
            and f.key not in ('message', 'contract')
        {% endif %}
    GROUP BY
        block_number,
        {% if sei_traces_mode %}
            tx_hash,
        {% else %}
            tx_position,
        {% endif %}
        partition_key,
        trace_address,
        _inserted_timestamp
)
SELECT
    block_number,
    {% if sei_traces_mode %}
        tx_hash,
    {% else %}
        tx_position,
    {% endif %}
    trace_address,
    parent_trace_address,
    trace_address_array,
    trace_json,
    partition_key,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number'] + 
        (['tx_hash'] if sei_traces_mode else ['tx_position']) + 
        ['trace_address']
    ) }} AS traces_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    flatten_traces qualify(ROW_NUMBER() over(PARTITION BY traces_id
ORDER BY
    _inserted_timestamp DESC)) = 1