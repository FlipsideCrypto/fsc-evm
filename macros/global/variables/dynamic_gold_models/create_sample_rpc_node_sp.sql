{% macro create_sample_rpc_node_sp() %}
    {% if var("UPDATE_UDFS_AND_SPS", false) %}
        {% if target.database.lower() in ['fsc_evm', 'fsc_evm_dev'] %}

            {% set create_admin_schema_sql %}
                create schema if not exists admin;
            {% endset %}

            {% do run_query(create_admin_schema_sql) %}

            {% set create_admin_logs_table_sql %}
                create table if not exists admin.rpc_node_logs (
                    log_id number autoincrement,
                    inserted_at timestamp_ntz default sysdate(),
                    blockchain string,
                    network string,
                    receipts_by_block boolean,
                    blocks_per_hour number,
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

            CREATE OR REPLACE PROCEDURE admin.sample_rpc_node(
                BLOCKCHAIN STRING,
                NODE_PROVIDER STRING,
                NETWORK STRING DEFAULT 'mainnet',
                RANDOM_BLOCK_SAMPLE_SIZE NUMBER DEFAULT 50,
                VAULT_PATH_OVERRIDE STRING DEFAULT NULL,
                NODE_URL_OVERRIDE STRING DEFAULT NULL,
                EXCLUDE_TRACES BOOLEAN DEFAULT FALSE
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
                    WITH node_provider_details as (
                        SELECT
                            CASE
                                WHEN :VAULT_PATH_OVERRIDE IS NOT NULL then :VAULT_PATH_OVERRIDE
                                WHEN lower(:NODE_PROVIDER) IN ('drpc') then 'Vault/prod/evm/drpc'
                                WHEN lower(:NODE_PROVIDER) IN ('quicknode') then 'Vault/prod/evm/quicknode/' || :BLOCKCHAIN || '/' || :NETWORK
                                ELSE ''
                            END as vault_path,
                            CASE
                                WHEN :NODE_URL_OVERRIDE IS NOT NULL then :NODE_URL_OVERRIDE
                                WHEN lower(:NODE_PROVIDER) IN ('drpc') and lower(:NETWORK) = 'mainnet' then 'https://lb.drpc.org/ogrpc?network=' || :BLOCKCHAIN || '&dkey={KEY}'
                                WHEN lower(:NODE_PROVIDER) IN ('drpc') and lower(:NETWORK) <> 'mainnet' then 'https://lb.drpc.org/ogrpc?network=' || :BLOCKCHAIN || '-' || :NETWORK || '&dkey={KEY}'
                                WHEN lower(:NODE_PROVIDER) IN ('quicknode') then '{URL}'
                            END as node_url
                    ),
                    chainhead AS (
                        SELECT utils.udf_hex_to_int(
                            live.udf_api(
                                'POST',
                                node_url,
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
                                vault_path
                            ):data:result::string
                        )::int as block_number
                        FROM node_provider_details
                    ),
                    recent_blocks AS (
                        SELECT 
                            chainhead.block_number - seq4() as block_num
                        FROM chainhead, TABLE(GENERATOR(ROWCOUNT => 10))
                        WHERE chainhead.block_number - seq4() > 0
                    ),
                    random_blocks AS (
                        SELECT 
                            MOD(ABS(RANDOM()), (SELECT block_number FROM chainhead)) + 1 as block_num
                        FROM TABLE(GENERATOR(ROWCOUNT => :RANDOM_BLOCK_SAMPLE_SIZE))
                        WHERE block_num > 0
                    ),
                    random_numbers AS (
                        SELECT block_num as random_num FROM recent_blocks
                        UNION ALL
                        SELECT block_num as random_num FROM random_blocks
                    ),
                    block_range as (
                        SELECT array_agg(random_num) as block_range FROM random_numbers
                    ),
                    sample_receipts AS (
                        SELECT 
                            random_num as block_number,
                            live.udf_api(
                                'POST',
                                node_url,
                                OBJECT_CONSTRUCT(
                                    'Content-Type', 'application/json',
                                    'fsc-quantum-state', 'LiveQuery'
                                ),
                                OBJECT_CONSTRUCT(
                                    'id', random_num,
                                    'jsonrpc', '2.0',
                                    'method', 'eth_getBlockReceipts',
                                    'params', ARRAY_CONSTRUCT(utils.udf_int_to_hex(random_num))
                                ),
                                vault_path
                            ):data:result as response
                        FROM random_numbers
                        JOIN node_provider_details ON 1=1
                    ),
                    sample_blocks AS (
                        SELECT 
                            random_num as block_number,
                            live.udf_api(
                                'POST',
                                node_url,
                                OBJECT_CONSTRUCT(
                                    'Content-Type', 'application/json',
                                    'fsc-quantum-state', 'LiveQuery'
                                ),
                                OBJECT_CONSTRUCT(
                                    'id', random_num,
                                    'jsonrpc', '2.0',
                                    'method', 'eth_getBlockByNumber',
                                    'params', ARRAY_CONSTRUCT(utils.udf_int_to_hex(random_num), true)
                                ),
                                vault_path
                            ):data:result as response
                        FROM random_numbers
                        JOIN node_provider_details ON 1=1
                    ),
                    sample_traces AS (
                        SELECT 
                            random_num as block_number,
                            IFF(NOT :EXCLUDE_TRACES,
                                live.udf_api(
                                    'POST',
                                    node_url,
                                    OBJECT_CONSTRUCT(
                                        'Content-Type', 'application/json',
                                        'fsc-quantum-state', 'LiveQuery'
                                    ),
                                    OBJECT_CONSTRUCT(
                                        'id', random_num,
                                        'jsonrpc', '2.0',
                                        'method', 'debug_traceBlockByNumber',
                                        'params', ARRAY_CONSTRUCT(
                                            utils.udf_int_to_hex(random_num),
                                            OBJECT_CONSTRUCT('tracer', 'callTracer', 'timeout', '120s')
                                        )
                                    ),
                                    vault_path
                                ):data:result,
                                NULL
                            ) as response
                        FROM random_numbers
                        JOIN node_provider_details ON 1=1
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
                            utils.udf_hex_to_int(response:timestamp::string)::number as unix_timestamp
                        FROM sample_blocks
                        WHERE response is not null
                        ORDER BY block_number desc
                        limit 2
                    ),
                    min_max_blocks AS (
                        SELECT 
                            MIN(block_number) as min_block,
                            MAX(block_number) as max_block
                        FROM block_timestamps
                    ),
                    min_max_timestamps AS (
                        SELECT 
                            b1.unix_timestamp as min_timestamp,
                            b2.unix_timestamp as max_timestamp,
                            mm.min_block,
                            mm.max_block
                        FROM min_max_blocks mm
                        JOIN block_timestamps b1 ON b1.block_number = mm.min_block
                        JOIN block_timestamps b2 ON b2.block_number = mm.max_block
                    ),
                    block_time_analysis AS (
                        SELECT 
                            min_block,
                            max_block,
                            min_timestamp,
                            max_timestamp,
                            (max_timestamp - min_timestamp) / (max_block - min_block) as avg_block_time_seconds,
                            3600 / ((max_timestamp - min_timestamp) / (max_block - min_block)) as blocks_per_hour
                        FROM min_max_timestamps
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
                            'node_provider', :NODE_PROVIDER,
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
                INSERT INTO admin.rpc_node_logs (
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
                        'node_provider', :NODE_PROVIDER,
                        'error', 'Procedure execution failed: ' || SQLERRM
                    );
                    
                    -- Log the error result
                    INSERT INTO admin.rpc_node_logs (
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
            {% set permissions_sql %}
                grant usage on schema admin to internal_dev;
                grant usage on procedure admin.sample_rpc_node(string, string, string, number, string, string, boolean) to role internal_dev;
                grant usage on schema admin to dbt_cloud_fsc_evm;
                grant usage on procedure admin.sample_rpc_node(string, string, string, number, string, string, boolean) to role dbt_cloud_fsc_evm;
            {% endset %}

            {% do run_query(permissions_sql) %}

            {{ log("Created stored procedure: admin.sample_rpc_node", info=True) }}
        {% endif %}
    {% endif %}
{% endmacro %}