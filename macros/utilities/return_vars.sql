{% macro return_vars() %}

{# Global Variables #}
{%- set GLOBAL_PROD_DB_NAME = get_var('GLOBAL_PROD_DB_NAME', '') -%} --required
{%- set GLOBAL_CHAIN_NETWORK = get_var('GLOBAL_CHAIN_NETWORK', 'mainnet') -%}
{%- set GLOBAL_NODE_URL = get_var('GLOBAL_NODE_URL', '{URL}') -%}
{%- set GLOBAL_NODE_PROVIDER = get_var('GLOBAL_NODE_PROVIDER', '') -%} --required
{%- set GLOBAL_NODE_VAULT_PATH = get_var('GLOBAL_NODE_VAULT_PATH', 'Vault/prod/' ~ GLOBAL_PROD_DB_NAME.lower() ~ '/' ~ GLOBAL_NODE_PROVIDER.lower() ~ '/' ~ GLOBAL_CHAIN_NETWORK.lower()) -%} --this doesnt make sense, should be provider/chain/network

{%- set USES_RECEIPTS_BY_HASH = get_var('MAIN_CORE_RECEIPTS_BY_HASH_ENABLED', false) -%}
{%- set GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS = get_var('GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS','') -%} --required

{# Bronze Variables #}
{%- set bronze_partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER)" -%}
{%- set bronze_partition_join_key = 'partition_key' -%}
{%- set bronze_uses_receipts_by_hash = get_var('MAIN_CORE_RECEIPTS_BY_HASH_ENABLED', false) -%}

{# Silver Variables #}
{%- set MAIN_CORE_UNIQUE_KEY = 'tx_hash' if USES_RECEIPTS_BY_HASH else 'block_number' -%}
{%- set MAIN_CORE_RECEIPTS_SOURCE_NAME = 'RECEIPTS_BY_HASH' if USES_RECEIPTS_BY_HASH else 'RECEIPTS' -%}
{%- set MAIN_CORE_TRACES_BLOCKCHAIN_MODE = get_var('MAIN_CORE_TRACES_BLOCKCHAIN_MODE', none) -%}
{%- set MAIN_CORE_TRACES_FULL_RELOAD_ENABLED = get_var('MAIN_CORE_SILVER_TRACES_FULL_RELOAD_ENABLED', false) -%}
{%- set MAIN_CORE_TRACES_FULL_RELOAD_START_BLOCK = get_var('MAIN_CORE_SILVER_TRACES_FULL_RELOAD_START_BLOCK', 0) -%}
{%- set MAIN_CORE_TRACES_FULL_RELOAD_BLOCKS_PER_RUN = get_var('MAIN_CORE_SILVER_TRACES_FULL_RELOAD_BLOCKS_PER_RUN', 1000000) -%}

{# Gold Variables #}

{# Streamline Variables #}
{%- set MAIN_SL_TESTING_LIMIT = get_var('MAIN_SL_TESTING_LIMIT', none) -%}
{%- set MAIN_SL_NEW_BUILD_ENABLED = get_var('MAIN_SL_NEW_BUILD_ENABLED', false) -%}
{%- set MAIN_SL_BLOCKS_PER_HOUR = get_var('MAIN_SL_BLOCKS_PER_HOUR', 0) -%} --required
{%- set MAIN_SL_TRANSACTIONS_PER_BLOCK = get_var('MAIN_SL_TRANSACTIONS_PER_BLOCK', 0) -%}

{%- set MAIN_SL_BLOCKS_PER_HOUR = get_var('MAIN_SL_BLOCKS_PER_HOUR', 0) -%}
{%- set MAIN_SL_CHAINHEAD_DELAY_MINUTES = get_var('MAIN_SL_CHAINHEAD_DELAY_MINUTES', 3) -%}

{# Blocks Transactions Variables #}
{%- set MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_SQL_LIMIT = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_SQL_LIMIT', 2 * MAIN_SL_BLOCKS_PER_HOUR) -%}
{%- set MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_PRODUCER_BATCH_SIZE', 2 * MAIN_SL_BLOCKS_PER_HOUR) -%}
{%- set MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_WORKER_BATCH_SIZE = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_WORKER_BATCH_SIZE', MAIN_SL_BLOCKS_PER_HOUR) -%}
{%- set MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_ASYNC_CONCURRENT_REQUESTS', 100) -%}

{%- set MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_SQL_LIMIT = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_SQL_LIMIT', 1000 * MAIN_SL_BLOCKS_PER_HOUR) -%}
{%- set MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_PRODUCER_BATCH_SIZE', 10 * MAIN_SL_BLOCKS_PER_HOUR) -%}
{%- set MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_WORKER_BATCH_SIZE = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_WORKER_BATCH_SIZE', MAIN_SL_BLOCKS_PER_HOUR) -%}
{%- set MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_ASYNC_CONCURRENT_REQUESTS', 10) -%}

{# Receipts Variables #}
{%- set MAIN_SL_RECEIPTS_REALTIME_SQL_LIMIT = get_var('MAIN_SL_RECEIPTS_REALTIME_SQL_LIMIT', 2 * MAIN_SL_BLOCKS_PER_HOUR * MAIN_SL_TRANSACTIONS_PER_BLOCK) -%}
{%- set MAIN_SL_RECEIPTS_REALTIME_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_REALTIME_PRODUCER_BATCH_SIZE', 2 * MAIN_SL_BLOCKS_PER_HOUR * MAIN_SL_TRANSACTIONS_PER_BLOCK) -%}
{%- set MAIN_SL_RECEIPTS_REALTIME_WORKER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_REALTIME_WORKER_BATCH_SIZE', MAIN_SL_BLOCKS_PER_HOUR * MAIN_SL_TRANSACTIONS_PER_BLOCK) -%}
{%- set MAIN_SL_RECEIPTS_REALTIME_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_RECEIPTS_REALTIME_ASYNC_CONCURRENT_REQUESTS', 100) -%}

{%- set MAIN_SL_RECEIPTS_HISTORY_SQL_LIMIT = get_var('MAIN_SL_RECEIPTS_HISTORY_SQL_LIMIT', 1000 * MAIN_SL_BLOCKS_PER_HOUR * MAIN_SL_TRANSACTIONS_PER_BLOCK) -%}
{%- set MAIN_SL_RECEIPTS_HISTORY_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_HISTORY_PRODUCER_BATCH_SIZE', 10 * MAIN_SL_BLOCKS_PER_HOUR * MAIN_SL_TRANSACTIONS_PER_BLOCK) -%}
{%- set MAIN_SL_RECEIPTS_HISTORY_WORKER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_HISTORY_WORKER_BATCH_SIZE', MAIN_SL_BLOCKS_PER_HOUR * MAIN_SL_TRANSACTIONS_PER_BLOCK) -%}
{%- set MAIN_SL_RECEIPTS_HISTORY_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_RECEIPTS_HISTORY_ASYNC_CONCURRENT_REQUESTS', 10) -%}

{%- set MAIN_SL_RECEIPTS_BY_HASH_REALTIME_SQL_LIMIT = get_var('MAIN_SL_RECEIPTS_BY_HASH_REALTIME_SQL_LIMIT', 2 * MAIN_SL_BLOCKS_PER_HOUR * MAIN_SL_TRANSACTIONS_PER_BLOCK) -%}
{%- set MAIN_SL_RECEIPTS_BY_HASH_REALTIME_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_BY_HASH_REALTIME_PRODUCER_BATCH_SIZE', 2 * MAIN_SL_BLOCKS_PER_HOUR * MAIN_SL_TRANSACTIONS_PER_BLOCK) -%}
{%- set MAIN_SL_RECEIPTS_BY_HASH_REALTIME_WORKER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_BY_HASH_REALTIME_WORKER_BATCH_SIZE', MAIN_SL_BLOCKS_PER_HOUR * MAIN_SL_TRANSACTIONS_PER_BLOCK) -%}
{%- set MAIN_SL_RECEIPTS_BY_HASH_REALTIME_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_RECEIPTS_BY_HASH_REALTIME_ASYNC_CONCURRENT_REQUESTS', 100) -%}

{%- set MAIN_SL_RECEIPTS_BY_HASH_HISTORY_SQL_LIMIT = get_var('MAIN_SL_RECEIPTS_BY_HASH_HISTORY_SQL_LIMIT', 1000 * MAIN_SL_BLOCKS_PER_HOUR * MAIN_SL_TRANSACTIONS_PER_BLOCK) -%}
{%- set MAIN_SL_RECEIPTS_BY_HASH_HISTORY_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_BY_HASH_HISTORY_PRODUCER_BATCH_SIZE', 10 * MAIN_SL_BLOCKS_PER_HOUR * MAIN_SL_TRANSACTIONS_PER_BLOCK) -%}
{%- set MAIN_SL_RECEIPTS_BY_HASH_HISTORY_WORKER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_BY_HASH_HISTORY_WORKER_BATCH_SIZE', MAIN_SL_BLOCKS_PER_HOUR * MAIN_SL_TRANSACTIONS_PER_BLOCK) -%}
{%- set MAIN_SL_RECEIPTS_BY_HASH_HISTORY_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_RECEIPTS_BY_HASH_HISTORY_ASYNC_CONCURRENT_REQUESTS', 10) -%}

{# Traces Variables #}
{%- set MAIN_SL_TRACES_REALTIME_SQL_LIMIT = get_var('MAIN_SL_TRACES_REALTIME_SQL_LIMIT', 2 * MAIN_SL_BLOCKS_PER_HOUR * MAIN_SL_TRANSACTIONS_PER_BLOCK) -%}
{%- set MAIN_SL_TRACES_REALTIME_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_TRACES_REALTIME_PRODUCER_BATCH_SIZE', 2 * MAIN_SL_BLOCKS_PER_HOUR * MAIN_SL_TRANSACTIONS_PER_BLOCK) -%}
{%- set MAIN_SL_TRACES_REALTIME_WORKER_BATCH_SIZE = get_var('MAIN_SL_TRACES_REALTIME_WORKER_BATCH_SIZE', MAIN_SL_BLOCKS_PER_HOUR * MAIN_SL_TRANSACTIONS_PER_BLOCK) -%}
{%- set MAIN_SL_TRACES_REALTIME_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_TRACES_REALTIME_ASYNC_CONCURRENT_REQUESTS', 100) -%}

{%- set MAIN_SL_TRACES_HISTORY_SQL_LIMIT = get_var('MAIN_SL_TRACES_HISTORY_SQL_LIMIT', 1000 * MAIN_SL_BLOCKS_PER_HOUR * MAIN_SL_TRANSACTIONS_PER_BLOCK) -%}
{%- set MAIN_SL_TRACES_HISTORY_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_TRACES_HISTORY_PRODUCER_BATCH_SIZE', 10 * MAIN_SL_BLOCKS_PER_HOUR * MAIN_SL_TRANSACTIONS_PER_BLOCK) -%}
{%- set MAIN_SL_TRACES_HISTORY_WORKER_BATCH_SIZE = get_var('MAIN_SL_TRACES_HISTORY_WORKER_BATCH_SIZE', MAIN_SL_BLOCKS_PER_HOUR * MAIN_SL_TRANSACTIONS_PER_BLOCK) -%}
{%- set MAIN_SL_TRACES_HISTORY_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_TRACES_HISTORY_ASYNC_CONCURRENT_REQUESTS', 10) -%}

{# Confirm Blocks Variables #}
{%- set MAIN_SL_CONFIRM_BLOCKS_REALTIME_SQL_LIMIT = get_var('MAIN_SL_CONFIRM_BLOCKS_REALTIME_SQL_LIMIT', 2 * MAIN_SL_BLOCKS_PER_HOUR) -%}
{%- set MAIN_SL_CONFIRM_BLOCKS_REALTIME_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_CONFIRM_BLOCKS_REALTIME_PRODUCER_BATCH_SIZE', 2 * MAIN_SL_BLOCKS_PER_HOUR) -%}
{%- set MAIN_SL_CONFIRM_BLOCKS_REALTIME_WORKER_BATCH_SIZE = get_var('MAIN_SL_CONFIRM_BLOCKS_REALTIME_WORKER_BATCH_SIZE', MAIN_SL_BLOCKS_PER_HOUR) -%}
{%- set MAIN_SL_CONFIRM_BLOCKS_REALTIME_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_CONFIRM_BLOCKS_REALTIME_ASYNC_CONCURRENT_REQUESTS', 100) -%}

{%- set MAIN_SL_CONFIRM_BLOCKS_HISTORY_SQL_LIMIT = get_var('MAIN_SL_CONFIRM_BLOCKS_HISTORY_SQL_LIMIT', 1000 * MAIN_SL_BLOCKS_PER_HOUR) -%}
{%- set MAIN_SL_CONFIRM_BLOCKS_HISTORY_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_CONFIRM_BLOCKS_HISTORY_PRODUCER_BATCH_SIZE', 10 * MAIN_SL_BLOCKS_PER_HOUR) -%}
{%- set MAIN_SL_CONFIRM_BLOCKS_HISTORY_WORKER_BATCH_SIZE = get_var('MAIN_SL_CONFIRM_BLOCKS_HISTORY_WORKER_BATCH_SIZE', MAIN_SL_BLOCKS_PER_HOUR) -%}
{%- set MAIN_SL_CONFIRM_BLOCKS_HISTORY_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_CONFIRM_BLOCKS_HISTORY_ASYNC_CONCURRENT_REQUESTS', 10) -%}

{# Prices Variables #}
{%- set PRICES_START_DATE = get_var('MAIN_CORE_NATIVE_PRICES_START_DATE','2024-01-01') -%}

{# Make all variables available to the dbt context #}
{% set vars_dict = {
  'GLOBAL_PROD_DB_NAME': GLOBAL_PROD_DB_NAME,
  'GLOBAL_CHAIN_NETWORK': GLOBAL_CHAIN_NETWORK,
  'GLOBAL_NODE_URL': GLOBAL_NODE_URL,
  'GLOBAL_NODE_PROVIDER': GLOBAL_NODE_PROVIDER,
  'GLOBAL_NODE_VAULT_PATH': GLOBAL_NODE_VAULT_PATH,
  'USES_RECEIPTS_BY_HASH': USES_RECEIPTS_BY_HASH,
  'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS,
  'bronze_partition_function': bronze_partition_function,
  'bronze_partition_join_key': bronze_partition_join_key,
  'bronze_uses_receipts_by_hash': bronze_uses_receipts_by_hash,
  'MAIN_CORE_UNIQUE_KEY': MAIN_CORE_UNIQUE_KEY,
  'MAIN_CORE_RECEIPTS_SOURCE_NAME': MAIN_CORE_RECEIPTS_SOURCE_NAME,
  'MAIN_CORE_TRACES_BLOCKCHAIN_MODE': MAIN_CORE_TRACES_BLOCKCHAIN_MODE,
  'MAIN_CORE_TRACES_FULL_RELOAD_ENABLED': MAIN_CORE_TRACES_FULL_RELOAD_ENABLED,
  'MAIN_CORE_TRACES_FULL_RELOAD_START_BLOCK': MAIN_CORE_TRACES_FULL_RELOAD_START_BLOCK,
  'MAIN_CORE_TRACES_FULL_RELOAD_BLOCKS_PER_RUN': MAIN_CORE_TRACES_FULL_RELOAD_BLOCKS_PER_RUN,
  'MAIN_SL_TESTING_LIMIT': MAIN_SL_TESTING_LIMIT,
  'MAIN_SL_NEW_BUILD_ENABLED': MAIN_SL_NEW_BUILD_ENABLED,
  'MAIN_SL_BLOCKS_PER_HOUR': MAIN_SL_BLOCKS_PER_HOUR,
  'MAIN_SL_TRANSACTIONS_PER_BLOCK': MAIN_SL_TRANSACTIONS_PER_BLOCK,
  'MAIN_SL_CHAINHEAD_DELAY_MINUTES': MAIN_SL_CHAINHEAD_DELAY_MINUTES,
  'MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_SQL_LIMIT': MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_SQL_LIMIT,
  'MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_PRODUCER_BATCH_SIZE': MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_PRODUCER_BATCH_SIZE,
  'MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_WORKER_BATCH_SIZE': MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_WORKER_BATCH_SIZE,
  'MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_ASYNC_CONCURRENT_REQUESTS': MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_ASYNC_CONCURRENT_REQUESTS,
  'MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_SQL_LIMIT': MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_SQL_LIMIT,
  'MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_PRODUCER_BATCH_SIZE': MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_PRODUCER_BATCH_SIZE,
  'MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_WORKER_BATCH_SIZE': MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_WORKER_BATCH_SIZE,
  'MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_ASYNC_CONCURRENT_REQUESTS': MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_ASYNC_CONCURRENT_REQUESTS,
  'MAIN_SL_RECEIPTS_REALTIME_SQL_LIMIT': MAIN_SL_RECEIPTS_REALTIME_SQL_LIMIT,
  'MAIN_SL_RECEIPTS_REALTIME_PRODUCER_BATCH_SIZE': MAIN_SL_RECEIPTS_REALTIME_PRODUCER_BATCH_SIZE,
  'MAIN_SL_RECEIPTS_REALTIME_WORKER_BATCH_SIZE': MAIN_SL_RECEIPTS_REALTIME_WORKER_BATCH_SIZE,
  'MAIN_SL_RECEIPTS_REALTIME_ASYNC_CONCURRENT_REQUESTS': MAIN_SL_RECEIPTS_REALTIME_ASYNC_CONCURRENT_REQUESTS,
  'MAIN_SL_RECEIPTS_HISTORY_SQL_LIMIT': MAIN_SL_RECEIPTS_HISTORY_SQL_LIMIT,
  'MAIN_SL_RECEIPTS_HISTORY_PRODUCER_BATCH_SIZE': MAIN_SL_RECEIPTS_HISTORY_PRODUCER_BATCH_SIZE,
  'MAIN_SL_RECEIPTS_HISTORY_WORKER_BATCH_SIZE': MAIN_SL_RECEIPTS_HISTORY_WORKER_BATCH_SIZE,
  'MAIN_SL_RECEIPTS_HISTORY_ASYNC_CONCURRENT_REQUESTS': MAIN_SL_RECEIPTS_HISTORY_ASYNC_CONCURRENT_REQUESTS,
  'MAIN_SL_RECEIPTS_BY_HASH_REALTIME_SQL_LIMIT': MAIN_SL_RECEIPTS_BY_HASH_REALTIME_SQL_LIMIT,
  'MAIN_SL_RECEIPTS_BY_HASH_REALTIME_PRODUCER_BATCH_SIZE': MAIN_SL_RECEIPTS_BY_HASH_REALTIME_PRODUCER_BATCH_SIZE,
  'MAIN_SL_RECEIPTS_BY_HASH_REALTIME_WORKER_BATCH_SIZE': MAIN_SL_RECEIPTS_BY_HASH_REALTIME_WORKER_BATCH_SIZE,
  'MAIN_SL_RECEIPTS_BY_HASH_REALTIME_ASYNC_CONCURRENT_REQUESTS': MAIN_SL_RECEIPTS_BY_HASH_REALTIME_ASYNC_CONCURRENT_REQUESTS,
  'MAIN_SL_RECEIPTS_BY_HASH_HISTORY_SQL_LIMIT': MAIN_SL_RECEIPTS_BY_HASH_HISTORY_SQL_LIMIT,
  'MAIN_SL_RECEIPTS_BY_HASH_HISTORY_PRODUCER_BATCH_SIZE': MAIN_SL_RECEIPTS_BY_HASH_HISTORY_PRODUCER_BATCH_SIZE,
  'MAIN_SL_RECEIPTS_BY_HASH_HISTORY_WORKER_BATCH_SIZE': MAIN_SL_RECEIPTS_BY_HASH_HISTORY_WORKER_BATCH_SIZE,
  'MAIN_SL_RECEIPTS_BY_HASH_HISTORY_ASYNC_CONCURRENT_REQUESTS': MAIN_SL_RECEIPTS_BY_HASH_HISTORY_ASYNC_CONCURRENT_REQUESTS,
  'MAIN_SL_TRACES_REALTIME_SQL_LIMIT': MAIN_SL_TRACES_REALTIME_SQL_LIMIT,
  'MAIN_SL_TRACES_REALTIME_PRODUCER_BATCH_SIZE': MAIN_SL_TRACES_REALTIME_PRODUCER_BATCH_SIZE,
  'MAIN_SL_TRACES_REALTIME_WORKER_BATCH_SIZE': MAIN_SL_TRACES_REALTIME_WORKER_BATCH_SIZE,
  'MAIN_SL_TRACES_REALTIME_ASYNC_CONCURRENT_REQUESTS': MAIN_SL_TRACES_REALTIME_ASYNC_CONCURRENT_REQUESTS,
  'MAIN_SL_TRACES_HISTORY_SQL_LIMIT': MAIN_SL_TRACES_HISTORY_SQL_LIMIT,
  'MAIN_SL_TRACES_HISTORY_PRODUCER_BATCH_SIZE': MAIN_SL_TRACES_HISTORY_PRODUCER_BATCH_SIZE,
  'MAIN_SL_TRACES_HISTORY_WORKER_BATCH_SIZE': MAIN_SL_TRACES_HISTORY_WORKER_BATCH_SIZE,
  'MAIN_SL_TRACES_HISTORY_ASYNC_CONCURRENT_REQUESTS': MAIN_SL_TRACES_HISTORY_ASYNC_CONCURRENT_REQUESTS,
  'MAIN_SL_CONFIRM_BLOCKS_REALTIME_SQL_LIMIT': MAIN_SL_CONFIRM_BLOCKS_REALTIME_SQL_LIMIT,
  'MAIN_SL_CONFIRM_BLOCKS_REALTIME_PRODUCER_BATCH_SIZE': MAIN_SL_CONFIRM_BLOCKS_REALTIME_PRODUCER_BATCH_SIZE,
  'MAIN_SL_CONFIRM_BLOCKS_REALTIME_WORKER_BATCH_SIZE': MAIN_SL_CONFIRM_BLOCKS_REALTIME_WORKER_BATCH_SIZE,
  'MAIN_SL_CONFIRM_BLOCKS_REALTIME_ASYNC_CONCURRENT_REQUESTS': MAIN_SL_CONFIRM_BLOCKS_REALTIME_ASYNC_CONCURRENT_REQUESTS,
  'MAIN_SL_CONFIRM_BLOCKS_HISTORY_SQL_LIMIT': MAIN_SL_CONFIRM_BLOCKS_HISTORY_SQL_LIMIT,
  'MAIN_SL_CONFIRM_BLOCKS_HISTORY_PRODUCER_BATCH_SIZE': MAIN_SL_CONFIRM_BLOCKS_HISTORY_PRODUCER_BATCH_SIZE,
  'MAIN_SL_CONFIRM_BLOCKS_HISTORY_WORKER_BATCH_SIZE': MAIN_SL_CONFIRM_BLOCKS_HISTORY_WORKER_BATCH_SIZE,
  'MAIN_SL_CONFIRM_BLOCKS_HISTORY_ASYNC_CONCURRENT_REQUESTS': MAIN_SL_CONFIRM_BLOCKS_HISTORY_ASYNC_CONCURRENT_REQUESTS,
  'PRICES_START_DATE': PRICES_START_DATE
} %}

{# Add all variables to the project_dict #}
{% do project_dict.update(vars_dict) %}
  
{# Return the dictionary for direct use in on-run-start #}
{{ return(vars_dict) }}

{% endmacro %}