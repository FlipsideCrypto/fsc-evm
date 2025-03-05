{% macro master_vars_keys() %}
    {% set master_keys = {
        'GLOBAL': {
            'PROD': {
                'GLOBAL_PROD_DB_NAME': {
                    'data_type': 'STRING',
                    'default': '\'\''
                },
            },
            'NODE': {
                'GLOBAL_NODE_URL': {
                    'data_type': 'STRING',
                    'default': '\'{Service}/{Authentication}\''
                },
                'GLOBAL_NODE_VAULT_PATH': {
                    'data_type': 'STRING',
                    'default': '\'\''
                },
            },
        },
        'MAIN': {
            'CORE': {
                'MAIN_CORE_RECEIPTS_BY_HASH_ENABLED': {
                    'data_type': 'BOOLEAN',
                    'default': 'false'
                },
                'MAIN_CORE_TRACES_BLOCKCHAIN_MODE': {
                    'data_type': 'NULL',
                    'default': 'none'
                },
                'MAIN_CORE_SILVER_TRACES_FULL_RELOAD_ENABLED': {
                    'data_type': 'BOOLEAN',
                    'default': 'false'
                },
                'MAIN_CORE_SILVER_TRACES_FULL_RELOAD_START_BLOCK': {
                    'data_type': 'NUMBER',
                    'default': '0'
                },
                'MAIN_CORE_SILVER_TRACES_FULL_RELOAD_BLOCKS_PER_RUN': {
                    'data_type': 'NUMBER',
                    'default': '1000000'
                },
            },
            'SL': {
                'MAIN_SL_TESTING_LIMIT': {
                    'data_type': 'NULL',
                    'default': 'none'
                },
                'MAIN_SL_NEW_BUILD_ENABLED': {
                    'data_type': 'BOOLEAN',
                    'default': 'false'
                },
                'MAIN_SL_BLOCKS_PER_HOUR': {
                    'data_type': 'NUMBER',
                    'default': '0'
                },
                'MAIN_SL_TRANSACTIONS_PER_BLOCK': {
                    'data_type': 'NUMBER',
                    'default': '0'
                },
                'MAIN_SL_MIN_BLOCK': {
                    'data_type': 'NULL',
                    'default': 'none'
                },
                'MAIN_SL_CHAINHEAD_DELAY_MINUTES': {
                    'data_type': 'NUMBER',
                    'default': '3'
                },
                'MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_SQL_LIMIT': {
                    'data_type': 'STRING',
                    'default': '2 * blocks_per_hour'
                },
                'MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_PRODUCER_BATCH_SIZE': {
                    'data_type': 'STRING',
                    'default': '2 * blocks_per_hour'
                },
                'MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_WORKER_BATCH_SIZE': {
                    'data_type': 'STRING',
                    'default': 'blocks_per_hour'
                },
                'MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_ASYNC_CONCURRENT_REQUESTS': {
                    'data_type': 'NUMBER',
                    'default': '100'
                },
                'MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_SQL_LIMIT': {
                    'data_type': 'STRING',
                    'default': '1000 * blocks_per_hour'
                },
                'MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_PRODUCER_BATCH_SIZE': {
                    'data_type': 'STRING',
                    'default': '10 * blocks_per_hour'
                },
                'MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_WORKER_BATCH_SIZE': {
                    'data_type': 'STRING',
                    'default': 'blocks_per_hour'
                },
                'MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_ASYNC_CONCURRENT_REQUESTS': {
                    'data_type': 'NUMBER',
                    'default': '10'
                },
                'MAIN_SL_RECEIPTS_REALTIME_SQL_LIMIT': {
                    'data_type': 'STRING',
                    'default': '2 * blocks_per_hour * transactions_per_block'
                },
                'MAIN_SL_RECEIPTS_REALTIME_PRODUCER_BATCH_SIZE': {
                    'data_type': 'STRING',
                    'default': '2 * blocks_per_hour * transactions_per_block'
                },
                'MAIN_SL_RECEIPTS_REALTIME_WORKER_BATCH_SIZE': {
                    'data_type': 'STRING',
                    'default': 'blocks_per_hour * transactions_per_block'
                },
                'MAIN_SL_RECEIPTS_REALTIME_ASYNC_CONCURRENT_REQUESTS': {
                    'data_type': 'NUMBER',
                    'default': '100'
                },
                'MAIN_SL_RECEIPTS_HISTORY_SQL_LIMIT': {
                    'data_type': 'STRING',
                    'default': '1000 * blocks_per_hour * transactions_per_block'
                },
                'MAIN_SL_RECEIPTS_HISTORY_PRODUCER_BATCH_SIZE': {
                    'data_type': 'STRING',
                    'default': '10 * blocks_per_hour * transactions_per_block'
                },
                'MAIN_SL_RECEIPTS_HISTORY_WORKER_BATCH_SIZE': {
                    'data_type': 'STRING',
                    'default': 'blocks_per_hour * transactions_per_block'
                },
                'MAIN_SL_RECEIPTS_HISTORY_ASYNC_CONCURRENT_REQUESTS': {
                    'data_type': 'NUMBER',
                    'default': '10'
                },
                'MAIN_SL_RECEIPTS_BY_HASH_REALTIME_SQL_LIMIT': {
                    'data_type': 'STRING',
                    'default': '2 * blocks_per_hour * transactions_per_block'
                },
                'MAIN_SL_RECEIPTS_BY_HASH_REALTIME_PRODUCER_BATCH_SIZE': {
                    'data_type': 'STRING',
                    'default': '2 * blocks_per_hour * transactions_per_block'
                },
                'MAIN_SL_RECEIPTS_BY_HASH_REALTIME_WORKER_BATCH_SIZE': {
                    'data_type': 'STRING',
                    'default': 'blocks_per_hour * transactions_per_block'
                },
                'MAIN_SL_RECEIPTS_BY_HASH_REALTIME_ASYNC_CONCURRENT_REQUESTS': {
                    'data_type': 'NUMBER',
                    'default': '100'
                },
                'MAIN_SL_RECEIPTS_BY_HASH_HISTORY_SQL_LIMIT': {
                    'data_type': 'STRING',
                    'default': '1000 * blocks_per_hour * transactions_per_block'
                },
                'MAIN_SL_RECEIPTS_BY_HASH_HISTORY_PRODUCER_BATCH_SIZE': {
                    'data_type': 'STRING',
                    'default': '10 * blocks_per_hour * transactions_per_block'
                },
                'MAIN_SL_RECEIPTS_BY_HASH_HISTORY_WORKER_BATCH_SIZE': {
                    'data_type': 'STRING',
                    'default': 'blocks_per_hour * transactions_per_block'
                },
                'MAIN_SL_RECEIPTS_BY_HASH_HISTORY_ASYNC_CONCURRENT_REQUESTS': {
                    'data_type': 'NUMBER',
                    'default': '10'
                },
                'MAIN_SL_TRACES_REALTIME_SQL_LIMIT': {
                    'data_type': 'STRING',
                    'default': '2 * blocks_per_hour * transactions_per_block'
                },
                'MAIN_SL_TRACES_REALTIME_PRODUCER_BATCH_SIZE': {
                    'data_type': 'STRING',
                    'default': '2 * blocks_per_hour * transactions_per_block'
                },
                'MAIN_SL_TRACES_REALTIME_WORKER_BATCH_SIZE': {
                    'data_type': 'STRING',
                    'default': 'blocks_per_hour * transactions_per_block'
                },
                'MAIN_SL_TRACES_REALTIME_ASYNC_CONCURRENT_REQUESTS': {
                    'data_type': 'NUMBER',
                    'default': '100'
                },
                'MAIN_SL_TRACES_HISTORY_SQL_LIMIT': {
                    'data_type': 'STRING',
                    'default': '1000 * blocks_per_hour * transactions_per_block'
                },
                'MAIN_SL_TRACES_HISTORY_PRODUCER_BATCH_SIZE': {
                    'data_type': 'STRING',
                    'default': '10 * blocks_per_hour * transactions_per_block'
                },
                'MAIN_SL_TRACES_HISTORY_WORKER_BATCH_SIZE': {
                    'data_type': 'STRING',
                    'default': 'blocks_per_hour * transactions_per_block'
                },
                'MAIN_SL_TRACES_HISTORY_ASYNC_CONCURRENT_REQUESTS': {
                    'data_type': 'NUMBER',
                    'default': '10'
                },
                'MAIN_SL_CONFIRM_BLOCKS_REALTIME_SQL_LIMIT': {
                    'data_type': 'STRING',
                    'default': '2 * blocks_per_hour'
                },
                'MAIN_SL_CONFIRM_BLOCKS_REALTIME_PRODUCER_BATCH_SIZE': {
                    'data_type': 'STRING',
                    'default': '2 * blocks_per_hour'
                },
                'MAIN_SL_CONFIRM_BLOCKS_REALTIME_WORKER_BATCH_SIZE': {
                    'data_type': 'STRING',
                    'default': 'blocks_per_hour'
                },
                'MAIN_SL_CONFIRM_BLOCKS_REALTIME_ASYNC_CONCURRENT_REQUESTS': {
                    'data_type': 'NUMBER',
                    'default': '100'
                },
                'MAIN_SL_CONFIRM_BLOCKS_HISTORY_SQL_LIMIT': {
                    'data_type': 'STRING',
                    'default': '1000 * blocks_per_hour'
                },
                'MAIN_SL_CONFIRM_BLOCKS_HISTORY_PRODUCER_BATCH_SIZE': {
                    'data_type': 'STRING',
                    'default': '10 * blocks_per_hour'
                },
                'MAIN_SL_CONFIRM_BLOCKS_HISTORY_WORKER_BATCH_SIZE': {
                    'data_type': 'STRING',
                    'default': 'blocks_per_hour'
                },
                'MAIN_SL_CONFIRM_BLOCKS_HISTORY_ASYNC_CONCURRENT_REQUESTS': {
                    'data_type': 'NUMBER',
                    'default': '10'
                },
            },
            'PRICES': {
                'MAIN_PRICES_NATIVE_SYMBOLS': {
                    'data_type': 'STRING',
                    'default': '\'\''
                },
                'MAIN_PRICES_NATIVE_BLOCKCHAINS': {
                    'data_type': 'STRING',
                    'default': 'get_var(\'GLOBAL_PROD_DB_NAME\', \'\').lower()'
                },
            },
        },
        'CURATED': {
            'VERTEX': {
                'CURATED_VERTEX_OFFCHAIN_EXCHANGE_CONTRACT': {
                    'data_type': 'STRING',
                    'default': '\'\''
                },
                'CURATED_VERTEX_CLEARINGHOUSE_CONTRACT': {
                    'data_type': 'STRING',
                    'default': '\'\''
                },
                'CURATED_VERTEX_TOKEN_MAPPING': {
                    'data_type': 'OBJECT',
                    'default': '{}'
                },
            },
        },
    } %}
    
    {{ return(master_keys) }}
{% endmacro %}