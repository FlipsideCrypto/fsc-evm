{% macro return_vars() %}

{# Global Variables #}
{%- set node_url = get_var('GLOBAL_NODE_URL', '{Service}/{Authentication}') -%}
{%- set node_secret_path = get_var('GLOBAL_NODE_VAULT_PATH', '') -%}
{%- set USES_RECEIPTS_BY_HASH = get_var('MAIN_CORE_RECEIPTS_BY_HASH_ENABLED', false) -%}

{# Bronze Variables #}

{%- set bronze_partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER)" -%}
{%- set bronze_partition_join_key = 'partition_key' -%}
{%- set bronze_uses_receipts_by_hash = get_var('MAIN_CORE_RECEIPTS_BY_HASH_ENABLED', false) -%}

{# Silver Variables #}

{%- set raw_silver_fr_enabled = get_var('GLOBAL_SILVER_FR_ENABLED', false) -%} {# default is false, pass in true to enable #}
{%- set GLOBAL_SILVER_FR_ENABLED = none if raw_silver_fr_enabled else false -%} {# sets to none if true, still requires --full-refresh, otherwise will use incremental #}
{%- set MAIN_CORE_RECEIPTS_UNIQUE_KEY = 'tx_hash' if USES_RECEIPTS_BY_HASH else 'block_number' -%}
{%- set MAIN_CORE_RECEIPTS_SOURCE_NAME = 'RECEIPTS_BY_HASH' if USES_RECEIPTS_BY_HASH else 'RECEIPTS' -%}
{%- set MAIN_CORE_TRACES_BLOCKCHAIN_MODE = get_var('MAIN_CORE_TRACES_BLOCKCHAIN_MODE', none) -%}
{%- set MAIN_CORE_TRACES_FULL_RELOAD_ENABLED = get_var('MAIN_CORE_SILVER_TRACES_FULL_RELOAD_ENABLED', false) -%}
{%- set MAIN_CORE_TRACES_FULL_RELOAD_START_BLOCK = get_var('MAIN_CORE_SILVER_TRACES_FULL_RELOAD_START_BLOCK', 0) -%}
{%- set MAIN_CORE_TRACES_FULL_RELOAD_BLOCKS_PER_RUN = get_var('MAIN_CORE_SILVER_TRACES_FULL_RELOAD_BLOCKS_PER_RUN', 1000000) -%}


{# Streamline Variables #}

{%- set raw_streamline_fr_enabled = get_var('GLOBAL_STREAMLINE_FR_ENABLED', false) -%} 
{%- set GLOBAL_STREAMLINE_FR_ENABLED = none if raw_streamline_fr_enabled else false -%} 

{%- set MAIN_SL_TESTING_LIMIT = get_var('MAIN_SL_TESTING_LIMIT', none) -%}
{%- set MAIN_SL_NEW_BUILD_ENABLED = get_var('MAIN_SL_NEW_BUILD_ENABLED', false) -%}
{%- set MAIN_SL_BLOCKS_PER_HOUR = get_var('MAIN_SL_BLOCKS_PER_HOUR', 0) -%}
{%- set MAIN_SL_TRANSACTIONS_PER_BLOCK = get_var('MAIN_SL_TRANSACTIONS_PER_BLOCK', 0) -%}

{%- set MAIN_SL_BLOCKS_PER_HOUR = get_var('MAIN_SL_BLOCKS_PER_HOUR', 0) -%}
{%- set MAIN_SL_CHAINHEAD_DELAY_MINUTES = get_var('MAIN_SL_CHAINHEAD_DELAY_MINUTES', 3) -%}

{# Blocks Transactions Variables #}

{%- set MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_SQL_LIMIT = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_SQL_LIMIT', 2 * blocks_per_hour) -%}
{%- set MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_PRODUCER_BATCH_SIZE', 2 * blocks_per_hour) -%}
{%- set MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_WORKER_BATCH_SIZE = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_WORKER_BATCH_SIZE', blocks_per_hour) -%}
{%- set MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_ASYNC_CONCURRENT_REQUESTS', 100) -%}

{%- set MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_SQL_LIMIT = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_SQL_LIMIT', 1000 * blocks_per_hour) -%}
{%- set MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_PRODUCER_BATCH_SIZE', 10 * blocks_per_hour) -%}
{%- set MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_WORKER_BATCH_SIZE = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_WORKER_BATCH_SIZE', blocks_per_hour) -%}
{%- set MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_ASYNC_CONCURRENT_REQUESTS', 10) -%}

{# Receipts Variables #}

{%- set MAIN_SL_RECEIPTS_REALTIME_SQL_LIMIT = get_var('MAIN_SL_RECEIPTS_REALTIME_SQL_LIMIT', 2 * blocks_per_hour * transactions_per_block) -%}
{%- set MAIN_SL_RECEIPTS_REALTIME_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_REALTIME_PRODUCER_BATCH_SIZE', 2 * blocks_per_hour * transactions_per_block) -%}
{%- set MAIN_SL_RECEIPTS_REALTIME_WORKER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_REALTIME_WORKER_BATCH_SIZE', blocks_per_hour * transactions_per_block) -%}
{%- set MAIN_SL_RECEIPTS_REALTIME_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_RECEIPTS_REALTIME_ASYNC_CONCURRENT_REQUESTS', 100) -%}

{%- set MAIN_SL_RECEIPTS_HISTORY_SQL_LIMIT = get_var('MAIN_SL_RECEIPTS_HISTORY_SQL_LIMIT', 1000 * blocks_per_hour * transactions_per_block) -%}
{%- set MAIN_SL_RECEIPTS_HISTORY_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_HISTORY_PRODUCER_BATCH_SIZE', 10 * blocks_per_hour * transactions_per_block) -%}
{%- set MAIN_SL_RECEIPTS_HISTORY_WORKER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_HISTORY_WORKER_BATCH_SIZE', blocks_per_hour * transactions_per_block) -%}
{%- set MAIN_SL_RECEIPTS_HISTORY_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_RECEIPTS_HISTORY_ASYNC_CONCURRENT_REQUESTS', 10) -%}

{%- set MAIN_SL_RECEIPTS_BY_HASH_REALTIME_SQL_LIMIT = get_var('MAIN_SL_RECEIPTS_BY_HASH_REALTIME_SQL_LIMIT', 2 * blocks_per_hour * transactions_per_block) -%}
{%- set MAIN_SL_RECEIPTS_BY_HASH_REALTIME_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_BY_HASH_REALTIME_PRODUCER_BATCH_SIZE', 2 * blocks_per_hour * transactions_per_block) -%}
{%- set MAIN_SL_RECEIPTS_BY_HASH_REALTIME_WORKER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_BY_HASH_REALTIME_WORKER_BATCH_SIZE', blocks_per_hour * transactions_per_block) -%}
{%- set MAIN_SL_RECEIPTS_BY_HASH_REALTIME_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_RECEIPTS_BY_HASH_REALTIME_ASYNC_CONCURRENT_REQUESTS', 100) -%}

{%- set MAIN_SL_RECEIPTS_BY_HASH_HISTORY_SQL_LIMIT = get_var('MAIN_SL_RECEIPTS_BY_HASH_HISTORY_SQL_LIMIT', 1000 * blocks_per_hour * transactions_per_block) -%}
{%- set MAIN_SL_RECEIPTS_BY_HASH_HISTORY_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_BY_HASH_HISTORY_PRODUCER_BATCH_SIZE', 10 * blocks_per_hour * transactions_per_block) -%}
{%- set MAIN_SL_RECEIPTS_BY_HASH_HISTORY_WORKER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_BY_HASH_HISTORY_WORKER_BATCH_SIZE', blocks_per_hour * transactions_per_block) -%}
{%- set MAIN_SL_RECEIPTS_BY_HASH_HISTORY_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_RECEIPTS_BY_HASH_HISTORY_ASYNC_CONCURRENT_REQUESTS', 10) -%}

{# Traces Variables #}

{%- set MAIN_SL_TRACES_REALTIME_SQL_LIMIT = get_var('MAIN_SL_TRACES_REALTIME_SQL_LIMIT', 2 * blocks_per_hour * transactions_per_block) -%}
{%- set MAIN_SL_TRACES_REALTIME_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_TRACES_REALTIME_PRODUCER_BATCH_SIZE', 2 * blocks_per_hour * transactions_per_block) -%}
{%- set MAIN_SL_TRACES_REALTIME_WORKER_BATCH_SIZE = get_var('MAIN_SL_TRACES_REALTIME_WORKER_BATCH_SIZE', blocks_per_hour * transactions_per_block) -%}
{%- set MAIN_SL_TRACES_REALTIME_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_TRACES_REALTIME_ASYNC_CONCURRENT_REQUESTS', 100) -%}

{%- set MAIN_SL_TRACES_HISTORY_SQL_LIMIT = get_var('MAIN_SL_TRACES_HISTORY_SQL_LIMIT', 1000 * blocks_per_hour * transactions_per_block) -%}
{%- set MAIN_SL_TRACES_HISTORY_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_TRACES_HISTORY_PRODUCER_BATCH_SIZE', 10 * blocks_per_hour * transactions_per_block) -%}
{%- set MAIN_SL_TRACES_HISTORY_WORKER_BATCH_SIZE = get_var('MAIN_SL_TRACES_HISTORY_WORKER_BATCH_SIZE', blocks_per_hour * transactions_per_block) -%}
{%- set MAIN_SL_TRACES_HISTORY_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_TRACES_HISTORY_ASYNC_CONCURRENT_REQUESTS', 10) -%}

{# Confirm Blocks Variables #}

{%- set MAIN_SL_CONFIRM_BLOCKS_REALTIME_SQL_LIMIT = get_var('MAIN_SL_CONFIRM_BLOCKS_REALTIME_SQL_LIMIT', 2 * blocks_per_hour) -%}
{%- set MAIN_SL_CONFIRM_BLOCKS_REALTIME_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_CONFIRM_BLOCKS_REALTIME_PRODUCER_BATCH_SIZE', 2 * blocks_per_hour) -%}
{%- set MAIN_SL_CONFIRM_BLOCKS_REALTIME_WORKER_BATCH_SIZE = get_var('MAIN_SL_CONFIRM_BLOCKS_REALTIME_WORKER_BATCH_SIZE', blocks_per_hour) -%}
{%- set MAIN_SL_CONFIRM_BLOCKS_REALTIME_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_CONFIRM_BLOCKS_REALTIME_ASYNC_CONCURRENT_REQUESTS', 100) -%}

{%- set MAIN_SL_CONFIRM_BLOCKS_HISTORY_SQL_LIMIT = get_var('MAIN_SL_CONFIRM_BLOCKS_HISTORY_SQL_LIMIT', 1000 * blocks_per_hour) -%}
{%- set MAIN_SL_CONFIRM_BLOCKS_HISTORY_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_CONFIRM_BLOCKS_HISTORY_PRODUCER_BATCH_SIZE', 10 * blocks_per_hour) -%}
{%- set MAIN_SL_CONFIRM_BLOCKS_HISTORY_WORKER_BATCH_SIZE = get_var('MAIN_SL_CONFIRM_BLOCKS_HISTORY_WORKER_BATCH_SIZE', blocks_per_hour) -%}
{%- set MAIN_SL_CONFIRM_BLOCKS_HISTORY_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_CONFIRM_BLOCKS_HISTORY_ASYNC_CONCURRENT_REQUESTS', 10) -%}

{% endmacro %}