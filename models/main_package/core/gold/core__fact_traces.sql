{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = vars.MAIN_CORE_GOLD_TRACES_UNIQUE_KEY,
    cluster_by = ['block_timestamp::DATE'],
    -- post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(block_number,tx_hash,from_address,to_address,trace_address,type,origin_from_address,origin_to_address,origin_function_signature), SUBSTRING(input,output,type,trace_address)", -- Moved to daily_search_optimization maintenance model
    incremental_predicates = [fsc_evm.standard_predicate()],
    full_refresh = vars.GLOBAL_GOLD_FR_ENABLED,
    tags = ['gold','core','traces','phase_2']
) }}

WITH silver_traces AS (
    SELECT
            block_number,
            {% if vars.MAIN_CORE_TRACES_SEI_MODE %}
                tx_hash,
            {% else %}
                tx_position,
            {% endif %}
            trace_address,
            parent_trace_address,
            trace_address_array,
            trace_json,
            traces_id,
            'regular' AS source
        FROM
            {{ ref(
                'silver__traces'
            ) }}
        WHERE
            1 = 1

{% if is_incremental() and not vars.MAIN_CORE_GOLD_TRACES_FULL_RELOAD_ENABLED %}
AND modified_timestamp > (
    SELECT
        COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
    FROM
        {{ this }}
) {% elif is_incremental() and vars.MAIN_CORE_GOLD_TRACES_FULL_RELOAD_ENABLED %}
AND block_number BETWEEN (
    SELECT
        MAX(
            block_number
        )
    FROM
        {{ this }}
)
AND (
    SELECT
        MAX(
            block_number
        ) + {{ vars.MAIN_CORE_GOLD_TRACES_FULL_RELOAD_BLOCKS_PER_RUN }}
    FROM
        {{ this }}
)
{% else %}
    AND block_number <= {{ vars.MAIN_CORE_GOLD_TRACES_FR_MAX_BLOCK }}
{% endif %}

    {% if vars.MAIN_CORE_TRACES_ARB_MODE %}
    UNION ALL
    SELECT
        block_number,
        tx_position,
        trace_address,
        parent_trace_address,
        IFF(
            trace_address = 'ORIGIN',
            ARRAY_CONSTRUCT('ORIGIN'),
            trace_address_array
        ) AS trace_address_array,
        trace_json,
        traces_id,
        'arb_traces' AS source
    FROM
        silver.arb_traces -- intentionally not using ref() to avoid dependency on silver__arb_traces
    WHERE
        1 = 1

{% if is_incremental() and not vars.MAIN_CORE_GOLD_TRACES_FULL_RELOAD_ENABLED %}
AND modified_timestamp > (
    SELECT
        DATEADD('hour', -2, MAX(modified_timestamp))
    FROM
        {{ this }}) {% elif is_incremental() and vars.MAIN_CORE_GOLD_TRACES_FULL_RELOAD_ENABLED %}
        AND block_number BETWEEN (
            SELECT
                MAX(
                    block_number
                )
            FROM
                {{ this }}
        )
        AND (
            SELECT
                MAX(
                    block_number
                ) + {{ vars.MAIN_CORE_GOLD_TRACES_FULL_RELOAD_BLOCKS_PER_RUN }}
            FROM
                {{ this }}
        )
    {% else %}
        AND block_number <= {{ vars.MAIN_CORE_GOLD_TRACES_FR_MAX_BLOCK }}
    {% endif %}
    {% endif %}
),
sub_traces AS (
    SELECT
        block_number,
        {% if vars.MAIN_CORE_TRACES_SEI_MODE %}
            tx_hash,
        {% else %}
            tx_position,
        {% endif %}
        parent_trace_address,
        COUNT(*) AS sub_traces
    FROM
        silver_traces
    GROUP BY
        block_number,
        {% if vars.MAIN_CORE_TRACES_SEI_MODE %}
            tx_hash,
        {% else %}
            tx_position,
        {% endif %}
        parent_trace_address
),
trace_index_array AS (
    SELECT
        block_number,
        {% if vars.MAIN_CORE_TRACES_SEI_MODE %}
            tx_hash,
        {% else %}
            tx_position,
        {% endif %}
        trace_address,
        ARRAY_AGG(flat_value) AS number_array
    FROM
        (
            SELECT
                block_number,
                {% if vars.MAIN_CORE_TRACES_SEI_MODE %}
                    tx_hash,
                {% else %}
                    tx_position,
                {% endif %}
                trace_address,
                IFF(
                    VALUE :: STRING = 'ORIGIN',
                    -1,
                    VALUE :: INT
                ) AS flat_value
            FROM
                silver_traces,
                LATERAL FLATTEN (
                    input => trace_address_array
                )
        )
    GROUP BY
        block_number,
        {% if vars.MAIN_CORE_TRACES_SEI_MODE %}
            tx_hash,
        {% else %}
            tx_position,
        {% endif %}
        trace_address
),
trace_index_sub_traces AS (
    SELECT
        b.block_number,
        {% if vars.MAIN_CORE_TRACES_SEI_MODE %}
            b.tx_hash,
        {% else %}
            b.tx_position,
        {% endif %}
        b.trace_address,
        IFNULL(
            sub_traces,
            0
        ) AS sub_traces,
        number_array,
        ROW_NUMBER() over (
            PARTITION BY b.block_number,
            {% if vars.MAIN_CORE_TRACES_SEI_MODE %}
                b.tx_hash
            {% else %}
                b.tx_position
            {% endif %}
            ORDER BY
                number_array ASC
        ) - 1 AS trace_index,
        b.trace_json,
        b.traces_id,
        b.source
    FROM
        silver_traces b
        LEFT JOIN sub_traces s
        ON b.block_number = s.block_number
        AND {% if vars.MAIN_CORE_TRACES_SEI_MODE %}
                b.tx_hash = s.tx_hash
            {% else %}
                b.tx_position = s.tx_position
            {% endif %}
        AND b.trace_address = s.parent_trace_address
        JOIN trace_index_array n
        ON b.block_number = n.block_number
        AND {% if vars.MAIN_CORE_TRACES_SEI_MODE %}
                b.tx_hash = n.tx_hash
            {% else %}
                b.tx_position = n.tx_position
            {% endif %}
        AND b.trace_address = n.trace_address
),
errored_traces AS (
    SELECT
        block_number,
        {% if vars.MAIN_CORE_TRACES_SEI_MODE %}
            tx_hash,
        {% else %}
            tx_position,
        {% endif %}
        trace_address,
        trace_json
    FROM
        trace_index_sub_traces
    WHERE
        trace_json :error :: STRING IS NOT NULL
),
error_logic AS (
    SELECT
        b0.block_number,
        {% if vars.MAIN_CORE_TRACES_SEI_MODE %}
            b0.tx_hash,
        {% else %}
            b0.tx_position,
        {% endif %}
        b0.trace_address,
        b0.trace_json :error :: STRING AS error,
        b1.trace_json :error :: STRING AS any_error,
        b2.trace_json :error :: STRING AS origin_error
    FROM
        trace_index_sub_traces b0
        LEFT JOIN errored_traces b1
        ON b0.block_number = b1.block_number
        {% if vars.MAIN_CORE_TRACES_SEI_MODE %}
            AND b0.tx_hash = b1.tx_hash
        {% else %}
            AND b0.tx_position = b1.tx_position
        {% endif %}
        AND b0.trace_address RLIKE CONCAT('^', b1.trace_address, '(_[0-9]+)*$')
        LEFT JOIN errored_traces b2
        ON b0.block_number = b2.block_number
        {% if vars.MAIN_CORE_TRACES_SEI_MODE %}
            AND b0.tx_hash = b2.tx_hash
        {% else %}
            AND b0.tx_position = b2.tx_position
        {% endif %}
        AND b2.trace_address = 'ORIGIN'
),
aggregated_errors AS (
    SELECT
        block_number,
        {% if vars.MAIN_CORE_TRACES_SEI_MODE %}
            tx_hash,
        {% else %}
            tx_position,
        {% endif %}
        trace_address,
        error,
        IFF(MAX(any_error) IS NULL
        AND error IS NULL
        AND origin_error IS NULL, TRUE, FALSE) AS trace_succeeded
    FROM
        error_logic
    GROUP BY
        block_number,
        {% if vars.MAIN_CORE_TRACES_SEI_MODE %}
            tx_hash,
        {% else %}
            tx_position,
        {% endif %}
        trace_address,
        error,
        origin_error),
        json_traces AS {% if not vars.MAIN_CORE_TRACES_ARB_MODE %}
            (
                SELECT
                    block_number,
                    {% if vars.MAIN_CORE_TRACES_SEI_MODE %}
                        tx_hash,
                    {% else %}
                        tx_position,
                    {% endif %}
                    trace_address,
                    sub_traces,
                    number_array,
                    trace_index,
                    trace_succeeded,
                    trace_json :error :: STRING AS error_reason,
                    {% if vars.MAIN_CORE_TRACES_KAIA_MODE %}
                        coalesce(
                            trace_json :revertReason :: STRING,
                            trace_json :reverted :message :: STRING
                        ) AS revert_reason,
                    {% else %}
                        trace_json :revertReason :: STRING AS revert_reason,
                    {% endif %}
                    trace_json :from :: STRING AS from_address,
                    trace_json :to :: STRING AS to_address,
                    IFNULL(
                        trace_json :value :: STRING,
                        '0x0'
                    ) AS value_hex,
                    IFNULL(
                        utils.udf_hex_to_int(
                            trace_json :value :: STRING
                        ),
                        '0'
                    ) AS value_precise_raw,
                    utils.udf_decimal_adjust(
                        value_precise_raw,
                        18
                    ) AS value_precise,
                    value_precise :: FLOAT AS VALUE,
                    utils.udf_hex_to_int(
                        trace_json :gas :: STRING
                    ) :: INT AS gas,
                    utils.udf_hex_to_int(
                        trace_json :gasUsed :: STRING
                    ) :: INT AS gas_used,
                    trace_json :input :: STRING AS input,
                    trace_json :output :: STRING AS output,
                    trace_json :type :: STRING AS TYPE,
                    traces_id
                FROM
                    trace_index_sub_traces
                    JOIN aggregated_errors USING (
                        block_number,
                        {% if vars.MAIN_CORE_TRACES_SEI_MODE %}
                            tx_hash,
                        {% else %}
                            tx_position,
                        {% endif %}
                        trace_address
                    )
                {% else %}
                    (
                        SELECT
                            block_number,
                            tx_position,
                            trace_address,
                            sub_traces,
                            number_array,
                            trace_index,
                            trace_succeeded,
                            trace_json :error :: STRING AS error_reason,
                            trace_json :revertReason :: STRING AS revert_reason,
                            trace_json :from :: STRING AS from_address,
                            trace_json :to :: STRING AS to_address,
                            IFNULL(
                                trace_json :value :: STRING,
                                '0x0'
                            ) AS value_hex,
                            IFNULL(
                                utils.udf_hex_to_int(
                                    trace_json :value :: STRING
                                ),
                                '0'
                            ) AS value_precise_raw,
                            utils.udf_decimal_adjust(
                                value_precise_raw,
                                18
                            ) AS value_precise,
                            value_precise :: FLOAT AS VALUE,
                            utils.udf_hex_to_int(
                                trace_json :gas :: STRING
                            ) :: INT AS gas,
                            utils.udf_hex_to_int(
                                trace_json :gasUsed :: STRING
                            ) :: INT AS gas_used,
                            trace_json :input :: STRING AS input,
                            trace_json :output :: STRING AS output,
                            trace_json :type :: STRING AS TYPE,
                            traces_id,
                            trace_json :afterEVMTransfers AS after_evm_transfers,
                            trace_json :beforeEVMTransfers AS before_evm_transfers
                        FROM
                            trace_index_sub_traces t0
                            JOIN aggregated_errors USING (
                                block_number,
                                tx_position,
                                trace_address
                            )
                        WHERE
                            t0.source <> 'arb_traces'
                        UNION ALL
                        SELECT
                            block_number,
                            tx_position,
                            trace_address,
                            sub_traces,
                            number_array,
                            trace_index,
                            trace_succeeded,
                            trace_json :error :: STRING AS error_reason,
                            NULL AS revert_reason,
                            trace_json :action :from :: STRING AS from_address,
                            COALESCE(
                                trace_json :action :to :: STRING,
                                trace_json :result :address :: STRING
                            ) AS to_address,
                            IFNULL(
                                trace_json :action :value :: STRING,
                                '0x0'
                            ) AS value_hex,
                            IFNULL(
                                utils.udf_hex_to_int(
                                    trace_json :action :value :: STRING
                                ),
                                '0'
                            ) AS value_precise_raw,
                            utils.udf_decimal_adjust(
                                value_precise_raw,
                                18
                            ) AS value_precise,
                            value_precise :: FLOAT AS VALUE,
                            utils.udf_hex_to_int(
                                trace_json :action :gas :: STRING
                            ) :: INT AS gas,
                            IFNULL(
                                utils.udf_hex_to_int(
                                    trace_json :result :gasUsed :: STRING
                                ),
                                0
                            ) :: INT AS gas_used,
                            COALESCE(
                                trace_json :action :input :: STRING,
                                trace_json :action :init :: STRING
                            ) AS input,
                            COALESCE(
                                trace_json :result :output :: STRING,
                                trace_json :result :code :: STRING
                            ) AS output,
                            UPPER(
                                COALESCE(
                                    trace_json :action :callType :: STRING,
                                    trace_json :type :: STRING
                                )
                            ) AS TYPE,
                            traces_id,
                            NULL AS after_evm_transfers,
                            NULL AS before_evm_transfers
                        FROM
                            trace_index_sub_traces t0
                            JOIN aggregated_errors USING (
                                block_number,
                                tx_position,
                                trace_address
                            )
                        WHERE
                            t0.source = 'arb_traces'
                        {% endif %}
                    ),
                    incremental_traces AS (
                        SELECT
                            f.block_number,
                            {% if vars.MAIN_CORE_TRACES_SEI_MODE %}
                                f.tx_hash,
                            {% else %}
                                t.tx_hash,
                            {% endif %}
                            t.block_timestamp,
                            t.origin_function_signature,
                            t.from_address AS origin_from_address,
                            t.to_address AS origin_to_address,
                            {% if vars.MAIN_CORE_TRACES_SEI_MODE %}
                                t.position AS tx_position,
                            {% else %}
                            f.tx_position,
                            {% endif %}
                            f.trace_index,
                            f.from_address AS from_address,
                            f.to_address AS to_address,
                            f.value_hex,
                            f.value_precise_raw,
                            f.value_precise,
                            f.value,
                            f.gas,
                            f.gas_used,
                            f.input,
                            f.output,
                            f.type,
                            f.sub_traces,
                            f.error_reason,
                            f.revert_reason,
                            f.traces_id,
                            f.trace_succeeded,
                            f.trace_address,
                            {% if vars.MAIN_CORE_GOLD_TRACES_TX_STATUS_ENABLED %}
                            t.tx_status AS tx_succeeded
                            {% else %}
                            t.tx_succeeded
                            {% endif %}
                            {% if vars.MAIN_CORE_TRACES_ARB_MODE %},
                            f.before_evm_transfers,
                            f.after_evm_transfers
                        {% endif %}
                        FROM
                            json_traces f
                            LEFT OUTER JOIN {{ ref('core__fact_transactions') }}
                            t
                            ON {% if vars.MAIN_CORE_TRACES_SEI_MODE %}
                                f.tx_hash = t.tx_hash
                            {% else %}
                                f.tx_position = t.tx_position
                            {% endif %}
                            AND f.block_number = t.block_number

{% if is_incremental() and not vars.MAIN_CORE_GOLD_TRACES_FULL_RELOAD_ENABLED %}
AND t.block_timestamp >= (
    SELECT
        DATEADD('hour', -36, MAX(block_timestamp))
    FROM
        {{ this }})
    {% endif %}
)

{% if is_incremental() %},
heal_missing_data AS (
    SELECT
        t.block_number,
        {% if vars.MAIN_CORE_TRACES_SEI_MODE %}
            t.tx_hash,
        {% else %}
            txs.tx_hash,
        {% endif %}
        txs.block_timestamp AS block_timestamp_heal,
        txs.origin_function_signature AS origin_function_signature_heal,
        txs.from_address AS origin_from_address_heal,
        txs.to_address AS origin_to_address_heal,
        {% if vars.MAIN_CORE_TRACES_SEI_MODE %}
            txs.position AS tx_position,
        {% else %}
            t.tx_position,
        {% endif %}
        t.trace_index,
        t.from_address,
        t.to_address,
        t.value_hex,
        t.value_precise_raw,
        t.value_precise,
        t.value,
        t.gas,
        t.gas_used,
        t.input,
        t.output,
        t.type,
        t.sub_traces,
        t.error_reason,
        t.revert_reason,
        t.fact_traces_id AS traces_id,
        t.trace_succeeded,
        t.trace_address,
        {% if vars.MAIN_CORE_GOLD_TRACES_TX_STATUS_ENABLED %}
        txs.tx_status AS tx_succeeded_heal
        {% else %}
        txs.tx_succeeded AS tx_succeeded_heal
        {% endif %}
        {% if vars.MAIN_CORE_TRACES_ARB_MODE %},
        t.before_evm_transfers,
        t.after_evm_transfers
    {% endif %}
    FROM
        {{ this }}
        t
        JOIN {{ ref('core__fact_transactions') }}
        txs
        {% if vars.MAIN_CORE_TRACES_SEI_MODE %}
            ON t.tx_hash = txs.tx_hash
        {% else %}
            ON t.tx_position = txs.tx_position
        {% endif %}
        AND t.block_number = txs.block_number
    WHERE
        {% if vars.MAIN_CORE_TRACES_SEI_MODE %}
            t.tx_position IS NULL
        {% else %}
            t.tx_hash IS NULL
        {% endif %}
        OR t.block_timestamp IS NULL
        OR t.tx_succeeded IS NULL
)
{% endif %},
all_traces AS (
    SELECT
        block_number,
        tx_hash,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_position,
        trace_index,
        from_address,
        to_address,
        value_hex,
        value_precise_raw,
        value_precise,
        VALUE,
        gas,
        gas_used,
        input,
        output,
        TYPE,
        sub_traces,
        error_reason,
        revert_reason,
        trace_succeeded,
        trace_address,
        tx_succeeded
    {% if vars.MAIN_CORE_TRACES_ARB_MODE %},
        before_evm_transfers,
        after_evm_transfers
    {% endif %}
    FROM
        incremental_traces

{% if is_incremental() %}
UNION ALL
SELECT
    block_number,
    tx_hash,
    block_timestamp_heal AS block_timestamp,
    origin_function_signature_heal AS origin_function_signature,
    origin_from_address_heal AS origin_from_address,
    origin_to_address_heal AS origin_to_address,
    tx_position,
    trace_index,
    from_address,
    to_address,
    value_hex,
    value_precise_raw,
    value_precise,
    VALUE,
    gas,
    gas_used,
    input,
    output,
    TYPE,
    sub_traces,
    error_reason,
    revert_reason,
    trace_succeeded,
    trace_address,
    tx_succeeded_heal AS tx_succeeded
{% if vars.MAIN_CORE_TRACES_ARB_MODE %},
    before_evm_transfers,
    after_evm_transfers
{% endif %}
FROM
    heal_missing_data
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    tx_position,
    trace_index,
    from_address,
    to_address,
    input,
    output,
    TYPE,
    trace_address,
    sub_traces,
    VALUE,
    value_precise_raw,
    value_precise,
    value_hex,
    gas,
    gas_used,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    {% if vars.MAIN_CORE_TRACES_ARB_MODE %}
        before_evm_transfers,
        after_evm_transfers,
    {% endif %}
    trace_succeeded,
    error_reason,
    revert_reason,
    tx_succeeded,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'trace_index']
    ) }} AS fact_traces_id,
    {% if is_incremental() or vars.GLOBAL_NEW_BUILD_ENABLED %}
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
    {% else %}
    CASE WHEN block_timestamp >= date_trunc('hour',SYSDATE()) - interval '6 hours' THEN SYSDATE() 
        ELSE GREATEST(block_timestamp, dateadd('day', -10, SYSDATE())) END AS inserted_timestamp,
    CASE WHEN block_timestamp >= date_trunc('hour',SYSDATE()) - interval '6 hours' THEN SYSDATE() 
        ELSE GREATEST(block_timestamp, dateadd('day', -10, SYSDATE())) END AS modified_timestamp
    {% endif %}
FROM
    all_traces qualify(ROW_NUMBER() over(PARTITION BY block_number,  {% if vars.MAIN_CORE_TRACES_SEI_MODE %}tx_hash, {% else %}tx_position, {% endif %} trace_index
ORDER BY
    modified_timestamp DESC, block_timestamp DESC nulls last)) = 1