-- depends_on: {{ ref('bronze__traces') }}
{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)",
    incremental_predicates = [fsc_evm.standard_predicate()],
    tags = ['silver_core']
) }}


    WITH bronze_traces AS (
        SELECT
            block_number,
            partition_key,
            VALUE :array_index :: INT AS tx_position,
            DATA :result AS full_traces,
            {% if MAIN_CORE_TRACES_BLOCKCHAIN_MODE == 'SEI' %}
                DATA :txHash :: STRING AS tx_hash,
            {% endif %}
            _inserted_timestamp
        FROM

{% if is_incremental() and not get_var('MAIN_CORE_TRACES_FULL_RELOAD_ENABLED') %}
{{ ref('bronze__traces') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(MAX(_inserted_timestamp), '1900-01-01') _inserted_timestamp
        FROM
            {{ this }}
    ) AND DATA :result IS NOT NULL 
    {% if get_var('MAIN_CORE_TRACES_BLOCKCHAIN_MODE') == 'ARB' %}
        AND block_number > 22207817
    {% endif %}

    {% elif is_incremental() and get_var('MAIN_CORE_TRACES_FULL_RELOAD_ENABLED') %}
    {{ ref('bronze__traces_fr') }}
WHERE
    
        partition_key BETWEEN (
            SELECT
                MAX(partition_key) - 100000
            FROM
                {{ this }}
        )
        AND (
            SELECT
                MAX(partition_key) + {{ get_var('MAIN_CORE_TRACES_FULL_RELOAD_BLOCKS_PER_RUN') }}
            FROM
                {{ this }}
        )

    {% if get_var('MAIN_CORE_TRACES_BLOCKCHAIN_MODE') == 'ARB' %}
        AND block_number > 22207817
    {% endif %}
{% else %}
    {{ ref('bronze__traces_fr') }}
WHERE
    partition_key <= {{ get_var('MAIN_CORE_TRACES_FULL_RELOAD_START_BLOCK') }}

    {% if get_var('MAIN_CORE_TRACES_BLOCKCHAIN_MODE') == 'ARB' %}
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
        {% if get_var('MAIN_CORE_TRACES_BLOCKCHAIN_MODE') == 'SEI' %}
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
                {% if get_var('MAIN_CORE_TRACES_BLOCKCHAIN_MODE') == 'ARB' %},
                    'afterEVMTransfers',
                    'beforeEVMTransfers',
                    'result.afterEVMTransfers',
                    'result.beforeEVMTransfers'
                {% endif %}
                {% if get_var('MAIN_CORE_TRACES_BLOCKCHAIN_MODE') == 'KAIA' %},
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
        {% if get_var('MAIN_CORE_TRACES_BLOCKCHAIN_MODE') == 'ARB' %}
            AND f.path NOT LIKE 'afterEVMTransfers[%'
            AND f.path NOT LIKE 'beforeEVMTransfers[%'
        {% endif %}
        {% if get_var('MAIN_CORE_TRACES_BLOCKCHAIN_MODE') == 'KAIA' %}
            and f.key not in ('message', 'contract')
        {% endif %}
    GROUP BY
        block_number,
        {% if get_var('MAIN_CORE_TRACES_BLOCKCHAIN_MODE') == 'SEI' %}
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
    {% if get_var('MAIN_CORE_TRACES_BLOCKCHAIN_MODE') == 'SEI' %}
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
        (['tx_hash'] if get_var('MAIN_CORE_TRACES_BLOCKCHAIN_MODE') == 'SEI' else ['tx_position']) + 
        ['trace_address']
    ) }} AS traces_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    flatten_traces qualify(ROW_NUMBER() over(PARTITION BY traces_id
ORDER BY
    _inserted_timestamp DESC)) = 1