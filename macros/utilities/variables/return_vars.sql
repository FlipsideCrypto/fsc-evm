{% macro return_vars() %}
  {# This macro sets and returns all configurable variables used throughout the project,
     organizing them by category (Global, Bronze, Silver, Streamline, etc.) with default values.
     IMPORTANT: Only call get_var() once per variable #}
  
  {# Set all variables on the namespace #}
  {% set ns = namespace() %}
  
  {# Set Variables and Default Values, organized by category #}
  
  {# Global Variables #}
  {% set ns.GLOBAL_PROD_DB_NAME = get_var('GLOBAL_PROD_DB_NAME', '') %}
  {% set ns.NODE_URL = get_var('GLOBAL_NODE_URL', '{Service}/{Authentication}') %}
  {% set ns.NODE_SECRET_PATH = get_var('GLOBAL_NODE_VAULT_PATH', '') %}
  {% set ns.USES_RECEIPTS_BY_HASH = get_var('MAIN_CORE_RECEIPTS_BY_HASH_ENABLED', false) %}
  
  {# Bronze Variables #}
  
  {% set ns.bronze_partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER)" %}
  {% set ns.bronze_partition_join_key = 'partition_key' %}
  {% set ns.bronze_uses_receipts_by_hash = get_var('MAIN_CORE_RECEIPTS_BY_HASH_ENABLED', false) %}
  
  {# Silver Variables #}
  
  {% set ns.raw_silver_fr_enabled = get_var('GLOBAL_SILVER_FR_ENABLED', false) %} {# default is false, pass in true to enable #}
  {% set ns.GLOBAL_SILVER_FR_ENABLED = none if ns.raw_silver_fr_enabled else false %} {# sets to none if true, still requires --full-refresh, otherwise will use incremental #}
  {% set ns.MAIN_CORE_RECEIPTS_UNIQUE_KEY = 'tx_hash' if ns.USES_RECEIPTS_BY_HASH else 'block_number' %}
  {% set ns.MAIN_CORE_RECEIPTS_SOURCE_NAME = 'RECEIPTS_BY_HASH' if ns.USES_RECEIPTS_BY_HASH else 'RECEIPTS' %}
  {% set ns.MAIN_CORE_TRACES_BLOCKCHAIN_MODE = get_var('MAIN_CORE_TRACES_BLOCKCHAIN_MODE', none) %}
  {% set ns.MAIN_CORE_TRACES_FULL_RELOAD_ENABLED = get_var('MAIN_CORE_SILVER_TRACES_FULL_RELOAD_ENABLED', false) %}
  {% set ns.MAIN_CORE_TRACES_FULL_RELOAD_START_BLOCK = get_var('MAIN_CORE_SILVER_TRACES_FULL_RELOAD_START_BLOCK', 0) %}
  {% set ns.MAIN_CORE_TRACES_FULL_RELOAD_BLOCKS_PER_RUN = get_var('MAIN_CORE_SILVER_TRACES_FULL_RELOAD_BLOCKS_PER_RUN', 1000000) %}
  
  {# Streamline Variables #}
  
  {% set blocks_per_hour = get_var('MAIN_SL_BLOCKS_PER_HOUR', 0) %}
  {% set transactions_per_block = get_var('MAIN_SL_TRANSACTIONS_PER_BLOCK', 0) %}
  
  {% set raw_streamline_fr_enabled = get_var('GLOBAL_STREAMLINE_FR_ENABLED', false) %} 
  {% set ns.GLOBAL_STREAMLINE_FR_ENABLED = none if raw_streamline_fr_enabled else false %} 
  
  {% set ns.MAIN_SL_TESTING_LIMIT = get_var('MAIN_SL_TESTING_LIMIT', none) %}
  {% set ns.MAIN_SL_NEW_BUILD_ENABLED = get_var('MAIN_SL_NEW_BUILD_ENABLED', false) %}
  {% set ns.MAIN_SL_BLOCKS_PER_HOUR = get_var('MAIN_SL_BLOCKS_PER_HOUR', 0) %}
  {% set ns.MAIN_SL_TRANSACTIONS_PER_BLOCK = get_var('MAIN_SL_TRANSACTIONS_PER_BLOCK', 0) %}
  {% set ns.MAIN_SL_MIN_BLOCK = get_var('MAIN_SL_MIN_BLOCK', none) %}
  
  {% set ns.MAIN_SL_BLOCKS_PER_HOUR = get_var('MAIN_SL_BLOCKS_PER_HOUR', 0) %}
  {% set ns.MAIN_SL_CHAINHEAD_DELAY_MINUTES = get_var('MAIN_SL_CHAINHEAD_DELAY_MINUTES', 3) %}
  
  {# Blocks Transactions Variables #}
  
  {% set ns.MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_SQL_LIMIT = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_SQL_LIMIT', 2 * blocks_per_hour) %}
  {% set ns.MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_PRODUCER_BATCH_SIZE', 2 * blocks_per_hour) %}
  {% set ns.MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_WORKER_BATCH_SIZE = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_WORKER_BATCH_SIZE', blocks_per_hour) %}
  {% set ns.MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_ASYNC_CONCURRENT_REQUESTS', 100) %}
  
  {% set ns.MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_SQL_LIMIT = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_SQL_LIMIT', 1000 * blocks_per_hour) %}
  {% set ns.MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_PRODUCER_BATCH_SIZE', 10 * blocks_per_hour) %}
  {% set ns.MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_WORKER_BATCH_SIZE = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_WORKER_BATCH_SIZE', blocks_per_hour) %}
  {% set ns.MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_ASYNC_CONCURRENT_REQUESTS', 10) %}
  
  {# Receipts Variables #}
  
  {% set ns.MAIN_SL_RECEIPTS_REALTIME_SQL_LIMIT = get_var('MAIN_SL_RECEIPTS_REALTIME_SQL_LIMIT', 2 * blocks_per_hour * transactions_per_block) %}
  {% set ns.MAIN_SL_RECEIPTS_REALTIME_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_REALTIME_PRODUCER_BATCH_SIZE', 2 * blocks_per_hour * transactions_per_block) %}
  {% set ns.MAIN_SL_RECEIPTS_REALTIME_WORKER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_REALTIME_WORKER_BATCH_SIZE', blocks_per_hour * transactions_per_block) %}
  {% set ns.MAIN_SL_RECEIPTS_REALTIME_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_RECEIPTS_REALTIME_ASYNC_CONCURRENT_REQUESTS', 100) %}
  
  {% set ns.MAIN_SL_RECEIPTS_HISTORY_SQL_LIMIT = get_var('MAIN_SL_RECEIPTS_HISTORY_SQL_LIMIT', 1000 * blocks_per_hour * transactions_per_block) %}
  {% set ns.MAIN_SL_RECEIPTS_HISTORY_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_HISTORY_PRODUCER_BATCH_SIZE', 10 * blocks_per_hour * transactions_per_block) %}
  {% set ns.MAIN_SL_RECEIPTS_HISTORY_WORKER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_HISTORY_WORKER_BATCH_SIZE', blocks_per_hour * transactions_per_block) %}
  {% set ns.MAIN_SL_RECEIPTS_HISTORY_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_RECEIPTS_HISTORY_ASYNC_CONCURRENT_REQUESTS', 10) %}
  
  {% set ns.MAIN_SL_RECEIPTS_BY_HASH_REALTIME_SQL_LIMIT = get_var('MAIN_SL_RECEIPTS_BY_HASH_REALTIME_SQL_LIMIT', 2 * blocks_per_hour * transactions_per_block) %}
  {% set ns.MAIN_SL_RECEIPTS_BY_HASH_REALTIME_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_BY_HASH_REALTIME_PRODUCER_BATCH_SIZE', 2 * blocks_per_hour * transactions_per_block) %}
  {% set ns.MAIN_SL_RECEIPTS_BY_HASH_REALTIME_WORKER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_BY_HASH_REALTIME_WORKER_BATCH_SIZE', blocks_per_hour * transactions_per_block) %}
  {% set ns.MAIN_SL_RECEIPTS_BY_HASH_REALTIME_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_RECEIPTS_BY_HASH_REALTIME_ASYNC_CONCURRENT_REQUESTS', 100) %}
  
  {% set ns.MAIN_SL_RECEIPTS_BY_HASH_HISTORY_SQL_LIMIT = get_var('MAIN_SL_RECEIPTS_BY_HASH_HISTORY_SQL_LIMIT', 1000 * blocks_per_hour * transactions_per_block) %}
  {% set ns.MAIN_SL_RECEIPTS_BY_HASH_HISTORY_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_BY_HASH_HISTORY_PRODUCER_BATCH_SIZE', 10 * blocks_per_hour * transactions_per_block) %}
  {% set ns.MAIN_SL_RECEIPTS_BY_HASH_HISTORY_WORKER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_BY_HASH_HISTORY_WORKER_BATCH_SIZE', blocks_per_hour * transactions_per_block) %}
  {% set ns.MAIN_SL_RECEIPTS_BY_HASH_HISTORY_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_RECEIPTS_BY_HASH_HISTORY_ASYNC_CONCURRENT_REQUESTS', 10) %}
  
  {# Traces Variables #}
  
  {% set ns.MAIN_SL_TRACES_REALTIME_SQL_LIMIT = get_var('MAIN_SL_TRACES_REALTIME_SQL_LIMIT', 2 * blocks_per_hour * transactions_per_block) %}
  {% set ns.MAIN_SL_TRACES_REALTIME_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_TRACES_REALTIME_PRODUCER_BATCH_SIZE', 2 * blocks_per_hour * transactions_per_block) %}
  {% set ns.MAIN_SL_TRACES_REALTIME_WORKER_BATCH_SIZE = get_var('MAIN_SL_TRACES_REALTIME_WORKER_BATCH_SIZE', blocks_per_hour * transactions_per_block) %}
  {% set ns.MAIN_SL_TRACES_REALTIME_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_TRACES_REALTIME_ASYNC_CONCURRENT_REQUESTS', 100) %}
  
  {% set ns.MAIN_SL_TRACES_HISTORY_SQL_LIMIT = get_var('MAIN_SL_TRACES_HISTORY_SQL_LIMIT', 1000 * blocks_per_hour * transactions_per_block) %}
  {% set ns.MAIN_SL_TRACES_HISTORY_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_TRACES_HISTORY_PRODUCER_BATCH_SIZE', 10 * blocks_per_hour * transactions_per_block) %}
  {% set ns.MAIN_SL_TRACES_HISTORY_WORKER_BATCH_SIZE = get_var('MAIN_SL_TRACES_HISTORY_WORKER_BATCH_SIZE', blocks_per_hour * transactions_per_block) %}
  {% set ns.MAIN_SL_TRACES_HISTORY_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_TRACES_HISTORY_ASYNC_CONCURRENT_REQUESTS', 10) %}
  
  {# Confirm Blocks Variables #}
  
  {% set ns.MAIN_SL_CONFIRM_BLOCKS_REALTIME_SQL_LIMIT = get_var('MAIN_SL_CONFIRM_BLOCKS_REALTIME_SQL_LIMIT', 2 * blocks_per_hour) %}
  {% set ns.MAIN_SL_CONFIRM_BLOCKS_REALTIME_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_CONFIRM_BLOCKS_REALTIME_PRODUCER_BATCH_SIZE', 2 * blocks_per_hour) %}
  {% set ns.MAIN_SL_CONFIRM_BLOCKS_REALTIME_WORKER_BATCH_SIZE = get_var('MAIN_SL_CONFIRM_BLOCKS_REALTIME_WORKER_BATCH_SIZE', blocks_per_hour) %}
  {% set ns.MAIN_SL_CONFIRM_BLOCKS_REALTIME_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_CONFIRM_BLOCKS_REALTIME_ASYNC_CONCURRENT_REQUESTS', 100) %}
  
  {% set ns.MAIN_SL_CONFIRM_BLOCKS_HISTORY_SQL_LIMIT = get_var('MAIN_SL_CONFIRM_BLOCKS_HISTORY_SQL_LIMIT', 1000 * blocks_per_hour) %}
  {% set ns.MAIN_SL_CONFIRM_BLOCKS_HISTORY_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_CONFIRM_BLOCKS_HISTORY_PRODUCER_BATCH_SIZE', 10 * blocks_per_hour) %}
  {% set ns.MAIN_SL_CONFIRM_BLOCKS_HISTORY_WORKER_BATCH_SIZE = get_var('MAIN_SL_CONFIRM_BLOCKS_HISTORY_WORKER_BATCH_SIZE', blocks_per_hour) %}
  {% set ns.MAIN_SL_CONFIRM_BLOCKS_HISTORY_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_CONFIRM_BLOCKS_HISTORY_ASYNC_CONCURRENT_REQUESTS', 10) %}
  
  {# Prices Variables #}
  
  {% set ns.MAIN_PRICES_NATIVE_SYMBOLS = get_var('MAIN_PRICES_NATIVE_SYMBOLS', '') %}
  {% set ns.MAIN_PRICES_NATIVE_BLOCKCHAINS = get_var('MAIN_PRICES_NATIVE_BLOCKCHAINS', ns.GLOBAL_PROD_DB_NAME.lower()) %}
  {% set ns.MAIN_PRICES_PROVIDER_PLATFORMS = get_var('MAIN_PRICES_PROVIDER_PLATFORMS', '') %}
  {% set ns.MAIN_PRICES_TOKEN_ADDRESSES = get_var('MAIN_PRICES_TOKEN_ADDRESSES', none) %}
  {% set ns.MAIN_PRICES_TOKEN_BLOCKCHAINS = get_var('MAIN_PRICES_TOKEN_BLOCKCHAINS', ns.GLOBAL_PROD_DB_NAME.lower()) %}
  
  {# Vertex Variables #}
  {% set ns.CURATED_VERTEX_OFFCHAIN_EXCHANGE_CONTRACT = get_var('CURATED_VERTEX_OFFCHAIN_EXCHANGE_CONTRACT', '') %}
  {% set ns.CURATED_VERTEX_CLEARINGHOUSE_CONTRACT = get_var('CURATED_VERTEX_CLEARINGHOUSE_CONTRACT', '') %}
  {% set ns.CURATED_VERTEX_TOKEN_MAPPING = get_var('CURATED_VERTEX_TOKEN_MAPPING', {}) %}
  
  {# Return the entire namespace as a dictionary #}
  {{ return(ns) }}
{% endmacro %}