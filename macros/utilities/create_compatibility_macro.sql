{% macro create_compatibility_macro() %}
    {% if var("UPDATE_UDFS_AND_SPS", false) %}

        {% set create_admin_schema_sql %}
            create schema if not exists admin;
        {% endset %}

        {% do run_query(create_admin_schema_sql) %}

        {% set create_admin_logs_table_sql %}
            create table if not exists admin.blockchain_compatibility_logs (
                log_id number autoincrement,
                inserted_at timestamp_ntz default sysdate(),
                blockchain string,
                network string,
                receipts_by_block boolean,
                blocks_per_hour float,
                result variant,
                blocks_fields array,
                transactions_fields array,
                receipts_fields array,
                traces_fields array,
                primary key (log_id)
            );
        {% endset %}

        {% do run_query(create_admin_logs_table_sql) %}
        

        {% set sp_compatibility_check_sql %}

        CREATE OR REPLACE PROCEDURE ADMIN.BLOCKCHAIN_COMPATIBILITY_CHECK(
            BLOCKCHAIN STRING,
            NETWORK STRING,
            NODE_URL STRING
        )
        RETURNS VARIANT
        LANGUAGE SQL
        AS
        $$
        DECLARE
            result VARIANT;
            create_table_stmt STRING;
        BEGIN
            -- Original logic to get the result, update to generic LQ when available
            result := (
                WITH chainhead AS (
                    SELECT monad.utils.udf_hex_to_int(
                        monad.live.udf_api(
                            'POST',
                            :NODE_URL,
                            OBJECT_CONSTRUCT(
                                'Content-Type', 'application/json',
                                'fsc-quantum-state', 'LiveQuery'
                            ),
                            OBJECT_CONSTRUCT(
                                'id', 1,
                                'jsonrpc', '2.0',
                                'method', 'eth_blockNumber',
                                'params', ARRAY_CONSTRUCT()
                            ),
                            ''
                        ):data:result::string
                    )::int as block_number
                ),
                random_numbers AS (
                    select random_num from (
                    SELECT 
                        MOD(ABS(RANDOM()) * ROW_NUMBER() OVER (ORDER BY RANDOM()), 
                            (SELECT block_number FROM chainhead)) + 1 as random_num
                    FROM TABLE(GENERATOR(ROWCOUNT => 50))
                    ) WHERE random_num > 0 
                ),
                block_range as (
                    select array_agg(random_num) as block_range from random_numbers
                ),
                sample_receipts AS (
                    SELECT 
                        random_num as block_number,
                        monad.live.udf_api(
                            'POST',
                            :NODE_URL,
                            OBJECT_CONSTRUCT(
                                'Content-Type', 'application/json',
                                'fsc-quantum-state', 'LiveQuery'
                            ),
                            OBJECT_CONSTRUCT(
                                'id', random_num,
                                'jsonrpc', '2.0',
                                'method', 'eth_getBlockReceipts',
                                'params', ARRAY_CONSTRUCT(monad.utils.udf_int_to_hex(random_num))
                            ),
                            ''
                        ):data:result as response
                    FROM random_numbers
                ),
                sample_blocks AS (
                    SELECT 
                        random_num as block_number,
                        monad.live.udf_api(
                            'POST',
                            :NODE_URL,
                            OBJECT_CONSTRUCT(
                                'Content-Type', 'application/json',
                                'fsc-quantum-state', 'LiveQuery'
                            ),
                            OBJECT_CONSTRUCT(
                                'id', random_num,
                                'jsonrpc', '2.0',
                                'method', 'eth_getBlockByNumber',
                                'params', ARRAY_CONSTRUCT(monad.utils.udf_int_to_hex(random_num), true)
                            ),
                            ''
                        ):data:result as response
                    FROM random_numbers
                ),
                sample_traces AS (
                    SELECT 
                        random_num as block_number,
                        monad.live.udf_api(
                            'POST',
                            :NODE_URL,
                            OBJECT_CONSTRUCT(
                                'Content-Type', 'application/json',
                                'fsc-quantum-state', 'LiveQuery'
                            ),
                            OBJECT_CONSTRUCT(
                                'id', random_num,
                                'jsonrpc', '2.0',
                                'method', 'debug_traceBlockByNumber',
                                'params', ARRAY_CONSTRUCT(
                                    monad.utils.udf_int_to_hex(random_num),
                                    OBJECT_CONSTRUCT('tracer', 'callTracer', 'timeout', '120s')
                                )
                            ),
                            ''
                        ):data:result as response
                    FROM random_numbers
                ),
                receipt_fields AS (
                    SELECT ARRAY_AGG(DISTINCT field_names.value::string) as fields
                    FROM sample_receipts,
                    LATERAL FLATTEN(input => response) as receipts,
                    LATERAL FLATTEN(input => OBJECT_KEYS(receipts.value)) as field_names
                    WHERE response is not null
                ),
                blocks_payload AS (
                    SELECT ARRAY_AGG(DISTINCT field_names.value::string) as fields
                    FROM sample_blocks,
                    LATERAL FLATTEN(input => OBJECT_KEYS(response)) as field_names
                    WHERE response is not null
                ),
                transactions_payload AS (
                    SELECT ARRAY_AGG(DISTINCT field_names.value::string) as fields
                    FROM sample_blocks,
                    LATERAL FLATTEN(input => response:transactions) as txs,
                    LATERAL FLATTEN(input => OBJECT_KEYS(txs.value)) as field_names
                    WHERE response:transactions is not null
                ),
                trace_fields AS (
                    SELECT ARRAY_AGG(DISTINCT key) as fields
                    FROM sample_traces txs,
                    TABLE(FLATTEN(
                        input => PARSE_JSON(txs.response),
                        recursive => TRUE
                    )) f
                    WHERE f.index IS NULL
                    AND f.key != 'calls'
                    AND f.path != 'result'
                ),
                block_timestamps AS (
                    SELECT 
                        block_number,
                        monad.utils.udf_hex_to_int(response:timestamp::string)::number as unix_timestamp
                    FROM sample_blocks
                    WHERE response is not null
                    ORDER BY block_number
                ),
                block_time_analysis AS (
            SELECT 
                -- Calculate time per block difference
                SUM(unix_timestamp_diff) / SUM(block_diff) as avg_block_time_seconds,
                -- Calculate blocks per hour
                3600 / (SUM(unix_timestamp_diff) / SUM(block_diff)) as blocks_per_hour
            FROM (
                SELECT 
                    block_number,
                    unix_timestamp,
                    CASE WHEN ROW_NUMBER() OVER (ORDER BY block_number) > 1 THEN 1 ELSE 0 END as is_valid_pair,
                    unix_timestamp - LAG(unix_timestamp, 1, unix_timestamp) OVER (ORDER BY block_number) as unix_timestamp_diff,
                    block_number - LAG(block_number, 1, block_number) OVER (ORDER BY block_number) as block_diff
                FROM block_timestamps
            )
            WHERE is_valid_pair = 1 
            AND block_diff > 0
        ),
                compatibility_check AS (
                    SELECT 
                        chainhead.block_number,
                        (SELECT COUNT(*) > 0 FROM sample_receipts WHERE response is not null) as supports_receipts,
                        (SELECT COUNT(*) > 0 FROM sample_blocks WHERE response is not null) as supports_blocks,
                        (SELECT COUNT(*) > 0 FROM sample_traces WHERE response is not null) as supports_traces,
                        block_range,
                        r.fields as receipt_fields,
                        b.fields as blocks_fields,
                        t.fields as transactions_fields,
                        tr.fields as trace_fields,
                        bta.avg_block_time_seconds,
                        bta.blocks_per_hour
                    FROM chainhead
                    JOIN block_range ON 1=1
                    LEFT JOIN receipt_fields r ON 1=1
                    LEFT JOIN blocks_payload b ON 1=1
                    LEFT JOIN transactions_payload t ON 1=1
                    LEFT JOIN trace_fields tr ON 1=1
                    LEFT JOIN block_time_analysis bta ON 1=1
                )
                SELECT 
                    OBJECT_CONSTRUCT(
                        'blockchain', :BLOCKCHAIN,
                        'network', :NETWORK,
                        'node_url', :NODE_URL,
                        'chainhead_block', block_number,
                        'eth_getBlockReceipts_supported', supports_receipts,
                        'eth_getBlockByNumber_supported', supports_blocks,
                        'debug_traceBlockByNumber_supported', supports_traces,
                        'range_tested', block_range,
                        'receipts_fields', receipt_fields,
                        'blocks_fields', blocks_fields,
                        'txs_fields', transactions_fields,
                        'traces_fields', trace_fields,
                        'avg_block_time_seconds', avg_block_time_seconds,
                        'blocks_per_hour', blocks_per_hour::float
                    )
                FROM compatibility_check
            );

            -- Log the result
            INSERT INTO BLOCKCHAIN_COMPATIBILITY_LOGS (
                BLOCKCHAIN,
                NETWORK,
                RECEIPTS_BY_BLOCK,
                BLOCKS_PER_HOUR,
                RESULT,
                BLOCKS_FIELDS,
                TRANSACTIONS_FIELDS,
                RECEIPTS_FIELDS,
                TRACES_FIELDS
            )
            SELECT 
                :BLOCKCHAIN,
                :NETWORK,
                :result:eth_getBlockReceipts_supported::boolean,
                :result:blocks_per_hour::float,
                :result,
                :result:blocks_fields::array,
                :result:txs_fields::array,
                :result:receipts_fields::array,
                :result:traces_fields::array
            FROM (SELECT :result as result);

            RETURN result;

        EXCEPTION
            WHEN OTHER THEN
                -- Create error result
                result := OBJECT_CONSTRUCT(
                    'blockchain', :BLOCKCHAIN,
                    'network', :NETWORK,
                    'node_url', :NODE_URL,
                    'error', 'Procedure execution failed: ' || SQLERRM
                );
                
                -- Log the error result
                INSERT INTO BLOCKCHAIN_COMPATIBILITY_LOGS (
                    BLOCKCHAIN,
                    NETWORK,
                    RECEIPTS_BY_BLOCK,
                    BLOCKS_PER_HOUR,
                    RESULT,
                    BLOCKS_FIELDS,
                    TRANSACTIONS_FIELDS,
                    RECEIPTS_FIELDS,
                    TRACES_FIELDS
                )
                SELECT 
                    :BLOCKCHAIN,
                    :NETWORK,
                    NULL, -- For error cases, we set RECEIPTS_BY_BLOCK to NULL
                    NULL, -- For error cases, we set BLOCKS_PER_HOUR to NULL
                    :result,
                    NULL, -- Arrays will be NULL for error cases
                    NULL,
                    NULL,
                    NULL
                FROM (SELECT :result as result);
                
                RETURN result;
        END;
        $$;
        {% endset %}

        {% do run_query(sp_compatibility_check_sql) %}
        {{ log("Created stored procedure: ADMIN.BLOCKCHAIN_COMPATIBILITY_CHECK", info=True) }}
    {% endif %}
{% endmacro %}