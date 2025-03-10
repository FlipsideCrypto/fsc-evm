{% macro return_vars() %}
  {# This macro sets and returns all configurable variables used throughout the project,
     organizing them by category (Global, Bronze, Silver, Streamline, etc.) with default values.
     IMPORTANT: Only call get_var() once per variable #}
  
  {# Set all variables on the namespace #}
  {% set ns = namespace() %}
  
  {# Set Variables and Default Values, organized by category #}
  
  {# Global Variables #}
  {% set ns.GLOBAL_PROJECT_NAME = get_var('GLOBAL_PROJECT_NAME', '') %}
  {% set ns.GLOBAL_NODE_URL = get_var('GLOBAL_NODE_URL', '{Service}/{Authentication}') %}
  {% set ns.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS = get_var('GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS', '') %}
  {% set ns.GLOBAL_MAX_SEQUENCE_NUMBER = get_var('GLOBAL_MAX_SEQUENCE_NUMBER', 1000000000) %}
  {% set ns.GLOBAL_NODE_SECRET_PATH = get_var('GLOBAL_NODE_VAULT_PATH', '') %}

  {% set ns.GLOBAL_BRONZE_FR_ENABLED = get_var('GLOBAL_BRONZE_FR_ENABLED', false) %} 
  {% set ns.GLOBAL_SILVER_FR_ENABLED = get_var('GLOBAL_SILVER_FR_ENABLED', false) %} 
  {% set ns.GLOBAL_GOLD_FR_ENABLED = get_var('GLOBAL_GOLD_FR_ENABLED', false) %} 
  {% set ns.GLOBAL_STREAMLINE_FR_ENABLED = get_var('GLOBAL_STREAMLINE_FR_ENABLED', false) %} 
  
  {# Core Variables #}
  {% set ns.MAIN_CORE_RECEIPTS_BY_HASH_ENABLED = get_var('MAIN_CORE_RECEIPTS_BY_HASH_ENABLED', false) %}

  {% set ns.MAIN_CORE_TRACES_ARB_MODE = ns.GLOBAL_PROJECT_NAME.upper() == 'ARBITRUM' %}
  {% set ns.MAIN_CORE_TRACES_SEI_MODE = ns.GLOBAL_PROJECT_NAME.upper() == 'SEI' %}
  {% set ns.MAIN_CORE_TRACES_KAIA_MODE = ns.GLOBAL_PROJECT_NAME.upper() == 'KAIA' %}

  {# Core Silver Variables #}
  {% set ns.MAIN_CORE_SILVER_RECEIPTS_UNIQUE_KEY = 'tx_hash' if ns.MAIN_CORE_RECEIPTS_BY_HASH_ENABLED else 'block_number' %}
  {% set ns.MAIN_CORE_SILVER_RECEIPTS_SOURCE_NAME = 'RECEIPTS_BY_HASH' if ns.MAIN_CORE_RECEIPTS_BY_HASH_ENABLED else 'RECEIPTS' %}
  {% set ns.MAIN_CORE_SILVER_RECEIPTS_POST_HOOK = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(tx_hash, block_number)" if ns.MAIN_CORE_RECEIPTS_BY_HASH_ENABLED else "" %}

  {% set ns.MAIN_CORE_SILVER_CONFIRM_BLOCKS_FULL_RELOAD_ENABLED = get_var('MAIN_CORE_SILVER_CONFIRM_BLOCKS_FULL_RELOAD_ENABLED', false) %}

  {% set ns.MAIN_CORE_SILVER_TRACES_FULL_RELOAD_ENABLED = get_var('MAIN_CORE_SILVER_TRACES_FULL_RELOAD_ENABLED', false) %}
  {% set ns.MAIN_CORE_SILVER_TRACES_FULL_RELOAD_START_BLOCK = get_var('MAIN_CORE_SILVER_TRACES_FULL_RELOAD_START_BLOCK', 0) %}
  {% set ns.MAIN_CORE_SILVER_TRACES_FULL_RELOAD_BLOCKS_PER_RUN = get_var('MAIN_CORE_SILVER_TRACES_FULL_RELOAD_BLOCKS_PER_RUN', 1000000) %}
  {% set ns.MAIN_CORE_SILVER_TRACES_PARTITION_KEY_ENABLED = get_var('MAIN_CORE_SILVER_TRACES_PARTITION_KEY_ENABLED', true) %}

  {# Core Gold Variables #}
  {% set ns.MAIN_CORE_GOLD_EZ_NATIVE_TRANSFERS_UNIQUE_KEY = 'tx_hash' if ns.MAIN_CORE_RECEIPTS_BY_HASH_ENABLED else 'block_number' %}
  {% set ns.MAIN_CORE_GOLD_EZ_NATIVE_TRANSFERS_PRICES_START_DATE = get_var('MAIN_CORE_GOLD_EZ_NATIVE_TRANSFERS_PRICES_START_DATE','2024-01-01') %}

  {% set ns.MAIN_CORE_GOLD_EZ_TOKEN_TRANSFERS_UNIQUE_KEY = 'tx_hash' if ns.MAIN_CORE_RECEIPTS_BY_HASH_ENABLED else 'block_number' %}

  {% set ns.MAIN_CORE_GOLD_FACT_EVENT_LOGS_UNIQUE_KEY = 'tx_hash' if ns.MAIN_CORE_RECEIPTS_BY_HASH_ENABLED else 'block_number' %}

  {% set ns.MAIN_CORE_GOLD_TRACES_FULL_RELOAD_ENABLED = get_var('MAIN_CORE_GOLD_TRACES_FULL_RELOAD_ENABLED', false) %}
  {% set ns.MAIN_CORE_GOLD_TRACES_FULL_RELOAD_START_BLOCK = get_var('MAIN_CORE_GOLD_TRACES_FULL_RELOAD_START_BLOCK', 0) %}
  {% set ns.MAIN_CORE_GOLD_TRACES_FULL_RELOAD_BLOCKS_PER_RUN = get_var('MAIN_CORE_GOLD_TRACES_FULL_RELOAD_BLOCKS_PER_RUN', 1000000) %}
  {% set ns.MAIN_CORE_GOLD_TRACES_OVERFLOW_ENABLED = get_var('MAIN_CORE_GOLD_TRACES_OVERFLOW_ENABLED', false) %}
  {% set ns.MAIN_CORE_GOLD_TRACES_TX_STATUS_ENABLED = get_var('MAIN_CORE_GOLD_TRACES_TX_STATUS_ENABLED', false) %}
  {% set ns.MAIN_CORE_GOLD_TRACES_SCHEMA_NAME = get_var('MAIN_CORE_GOLD_TRACES_SCHEMA_NAME', 'silver') %}
  {% if ns.MAIN_CORE_RECEIPTS_BY_HASH_ENABLED %}
    {% if ns.MAIN_CORE_TRACES_SEI_MODE %}
        {% set ns.MAIN_CORE_GOLD_TRACES_UNIQUE_KEY = "concat(block_number, '-', tx_hash)" %}
    {% else %}
        {% set ns.MAIN_CORE_GOLD_TRACES_UNIQUE_KEY = "concat(block_number, '-', tx_position)" %}
    {% endif %}
  {% else %}
      {% set ns.MAIN_CORE_GOLD_TRACES_UNIQUE_KEY = "block_number" %}
  {% endif %}

  {# Streamline Variables #}
  {% set ns.MAIN_SL_BLOCKS_PER_HOUR = get_var('MAIN_SL_BLOCKS_PER_HOUR', 1) %}
  {% set ns.MAIN_SL_TRANSACTIONS_PER_BLOCK = get_var('MAIN_SL_TRANSACTIONS_PER_BLOCK', 1) %}
  {% set ns.MAIN_SL_TESTING_LIMIT = get_var('MAIN_SL_TESTING_LIMIT', none) %}
  {% set ns.MAIN_SL_NEW_BUILD_ENABLED = get_var('MAIN_SL_NEW_BUILD_ENABLED', false) %}
  {% set ns.MAIN_SL_MIN_BLOCK = get_var('MAIN_SL_MIN_BLOCK', none) %}
  {% set ns.MAIN_SL_CHAINHEAD_DELAY_MINUTES = get_var('MAIN_SL_CHAINHEAD_DELAY_MINUTES', 3) %}
  {% set ns.MAIN_SL_BLOCK_LOOKBACK_ENABLED = get_var('MAIN_SL_BLOCK_LOOKBACK_ENABLED', true) %}
  
  {# SL Blocks Transactions Variables #}
  {% set ns.MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_SQL_LIMIT = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_SQL_LIMIT', 2 * ns.MAIN_SL_BLOCKS_PER_HOUR) %}
  {% set ns.MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_PRODUCER_BATCH_SIZE', 2 * ns.MAIN_SL_BLOCKS_PER_HOUR) %}
  {% set ns.MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_WORKER_BATCH_SIZE = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_WORKER_BATCH_SIZE', ns.MAIN_SL_BLOCKS_PER_HOUR) %}
  {% set ns.MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_ASYNC_CONCURRENT_REQUESTS', 100) %}
  
  {% set ns.MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_SQL_LIMIT = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_SQL_LIMIT', 1000 * ns.MAIN_SL_BLOCKS_PER_HOUR) %}
  {% set ns.MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_PRODUCER_BATCH_SIZE', 10 * ns.MAIN_SL_BLOCKS_PER_HOUR) %}
  {% set ns.MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_WORKER_BATCH_SIZE = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_WORKER_BATCH_SIZE', ns.MAIN_SL_BLOCKS_PER_HOUR) %}
  {% set ns.MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_ASYNC_CONCURRENT_REQUESTS', 10) %}
  
  {# SL Receipts Variables #}
  {% set ns.MAIN_SL_RECEIPTS_REALTIME_SQL_LIMIT = get_var('MAIN_SL_RECEIPTS_REALTIME_SQL_LIMIT', 2 * ns.MAIN_SL_BLOCKS_PER_HOUR) %}
  {% set ns.MAIN_SL_RECEIPTS_REALTIME_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_REALTIME_PRODUCER_BATCH_SIZE', 2 * ns.MAIN_SL_BLOCKS_PER_HOUR) %}
  {% set ns.MAIN_SL_RECEIPTS_REALTIME_WORKER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_REALTIME_WORKER_BATCH_SIZE', ns.MAIN_SL_BLOCKS_PER_HOUR) %}
  {% set ns.MAIN_SL_RECEIPTS_REALTIME_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_RECEIPTS_REALTIME_ASYNC_CONCURRENT_REQUESTS', 100) %}
  
  {% set ns.MAIN_SL_RECEIPTS_HISTORY_SQL_LIMIT = get_var('MAIN_SL_RECEIPTS_HISTORY_SQL_LIMIT', 1000 * ns.MAIN_SL_BLOCKS_PER_HOUR) %}
  {% set ns.MAIN_SL_RECEIPTS_HISTORY_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_HISTORY_PRODUCER_BATCH_SIZE', 10 * ns.MAIN_SL_BLOCKS_PER_HOUR) %}
  {% set ns.MAIN_SL_RECEIPTS_HISTORY_WORKER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_HISTORY_WORKER_BATCH_SIZE', ns.MAIN_SL_BLOCKS_PER_HOUR) %}
  {% set ns.MAIN_SL_RECEIPTS_HISTORY_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_RECEIPTS_HISTORY_ASYNC_CONCURRENT_REQUESTS', 10) %}
  
  {# SL Receipts By Hash Variables #}
  {% set ns.MAIN_SL_RECEIPTS_BY_HASH_REALTIME_SQL_LIMIT = get_var('MAIN_SL_RECEIPTS_BY_HASH_REALTIME_SQL_LIMIT', 2 * ns.MAIN_SL_BLOCKS_PER_HOUR * ns.MAIN_SL_TRANSACTIONS_PER_BLOCK) %}
  {% set ns.MAIN_SL_RECEIPTS_BY_HASH_REALTIME_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_BY_HASH_REALTIME_PRODUCER_BATCH_SIZE', 2 * ns.MAIN_SL_BLOCKS_PER_HOUR * ns.MAIN_SL_TRANSACTIONS_PER_BLOCK) %}
  {% set ns.MAIN_SL_RECEIPTS_BY_HASH_REALTIME_WORKER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_BY_HASH_REALTIME_WORKER_BATCH_SIZE', ns.MAIN_SL_BLOCKS_PER_HOUR * ns.MAIN_SL_TRANSACTIONS_PER_BLOCK) %}
  {% set ns.MAIN_SL_RECEIPTS_BY_HASH_REALTIME_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_RECEIPTS_BY_HASH_REALTIME_ASYNC_CONCURRENT_REQUESTS', 100) %}
  {% set ns.MAIN_SL_RECEIPTS_BY_HASH_REALTIME_TXNS_MODEL_ENABLED = get_var('MAIN_SL_RECEIPTS_BY_HASH_REALTIME_TXNS_MODEL_ENABLED', true) %}
  
  {% set ns.MAIN_SL_RECEIPTS_BY_HASH_HISTORY_SQL_LIMIT = get_var('MAIN_SL_RECEIPTS_BY_HASH_HISTORY_SQL_LIMIT', 1000 * ns.MAIN_SL_BLOCKS_PER_HOUR * ns.MAIN_SL_TRANSACTIONS_PER_BLOCK) %}
  {% set ns.MAIN_SL_RECEIPTS_BY_HASH_HISTORY_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_BY_HASH_HISTORY_PRODUCER_BATCH_SIZE', 10 * ns.MAIN_SL_BLOCKS_PER_HOUR * ns.MAIN_SL_TRANSACTIONS_PER_BLOCK) %}
  {% set ns.MAIN_SL_RECEIPTS_BY_HASH_HISTORY_WORKER_BATCH_SIZE = get_var('MAIN_SL_RECEIPTS_BY_HASH_HISTORY_WORKER_BATCH_SIZE', ns.MAIN_SL_BLOCKS_PER_HOUR * ns.MAIN_SL_TRANSACTIONS_PER_BLOCK) %}
  {% set ns.MAIN_SL_RECEIPTS_BY_HASH_HISTORY_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_RECEIPTS_BY_HASH_HISTORY_ASYNC_CONCURRENT_REQUESTS', 10) %}
  
  {# SL Traces Variables #}
  {% set ns.MAIN_SL_TRACES_REALTIME_SQL_LIMIT = get_var('MAIN_SL_TRACES_REALTIME_SQL_LIMIT', 2 * ns.MAIN_SL_BLOCKS_PER_HOUR) %}
  {% set ns.MAIN_SL_TRACES_REALTIME_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_TRACES_REALTIME_PRODUCER_BATCH_SIZE', 2 * ns.MAIN_SL_BLOCKS_PER_HOUR) %}
  {% set ns.MAIN_SL_TRACES_REALTIME_WORKER_BATCH_SIZE = get_var('MAIN_SL_TRACES_REALTIME_WORKER_BATCH_SIZE', ns.MAIN_SL_BLOCKS_PER_HOUR) %}
  {% set ns.MAIN_SL_TRACES_REALTIME_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_TRACES_REALTIME_ASYNC_CONCURRENT_REQUESTS', 100) %}
  {% set ns.MAIN_SL_TRACES_REALTIME_REQUEST_START_BLOCK = get_var('MAIN_SL_TRACES_REALTIME_REQUEST_START_BLOCK', none) %}
  
  {% set ns.MAIN_SL_TRACES_HISTORY_SQL_LIMIT = get_var('MAIN_SL_TRACES_HISTORY_SQL_LIMIT', 1000 * ns.MAIN_SL_BLOCKS_PER_HOUR) %}
  {% set ns.MAIN_SL_TRACES_HISTORY_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_TRACES_HISTORY_PRODUCER_BATCH_SIZE', 10 * ns.MAIN_SL_BLOCKS_PER_HOUR) %}
  {% set ns.MAIN_SL_TRACES_HISTORY_WORKER_BATCH_SIZE = get_var('MAIN_SL_TRACES_HISTORY_WORKER_BATCH_SIZE', ns.MAIN_SL_BLOCKS_PER_HOUR) %}
  {% set ns.MAIN_SL_TRACES_HISTORY_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_TRACES_HISTORY_ASYNC_CONCURRENT_REQUESTS', 10) %}
  {% set ns.MAIN_SL_TRACES_HISTORY_REQUEST_START_BLOCK = get_var('MAIN_SL_TRACES_HISTORY_REQUEST_START_BLOCK', none) %}
  
  {# SL Confirm Blocks Variables #}
  {% set ns.MAIN_SL_CONFIRM_BLOCKS_REALTIME_SQL_LIMIT = get_var('MAIN_SL_CONFIRM_BLOCKS_REALTIME_SQL_LIMIT', 2 * ns.MAIN_SL_BLOCKS_PER_HOUR) %}
  {% set ns.MAIN_SL_CONFIRM_BLOCKS_REALTIME_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_CONFIRM_BLOCKS_REALTIME_PRODUCER_BATCH_SIZE', 2 * ns.MAIN_SL_BLOCKS_PER_HOUR) %}
  {% set ns.MAIN_SL_CONFIRM_BLOCKS_REALTIME_WORKER_BATCH_SIZE = get_var('MAIN_SL_CONFIRM_BLOCKS_REALTIME_WORKER_BATCH_SIZE', ns.MAIN_SL_BLOCKS_PER_HOUR) %}
  {% set ns.MAIN_SL_CONFIRM_BLOCKS_REALTIME_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_CONFIRM_BLOCKS_REALTIME_ASYNC_CONCURRENT_REQUESTS', 100) %}
  
  {% set ns.MAIN_SL_CONFIRM_BLOCKS_HISTORY_SQL_LIMIT = get_var('MAIN_SL_CONFIRM_BLOCKS_HISTORY_SQL_LIMIT', 1000 * ns.MAIN_SL_BLOCKS_PER_HOUR) %}
  {% set ns.MAIN_SL_CONFIRM_BLOCKS_HISTORY_PRODUCER_BATCH_SIZE = get_var('MAIN_SL_CONFIRM_BLOCKS_HISTORY_PRODUCER_BATCH_SIZE', 10 * ns.MAIN_SL_BLOCKS_PER_HOUR) %}
  {% set ns.MAIN_SL_CONFIRM_BLOCKS_HISTORY_WORKER_BATCH_SIZE = get_var('MAIN_SL_CONFIRM_BLOCKS_HISTORY_WORKER_BATCH_SIZE', ns.MAIN_SL_BLOCKS_PER_HOUR) %}
  {% set ns.MAIN_SL_CONFIRM_BLOCKS_HISTORY_ASYNC_CONCURRENT_REQUESTS = get_var('MAIN_SL_CONFIRM_BLOCKS_HISTORY_ASYNC_CONCURRENT_REQUESTS', 10) %}

  {# SL Decoder Variables #}
  {% set ns.DECODER_SL_TESTING_LIMIT = get_var('DECODER_SL_TESTING_LIMIT', none) %}

  {# SL Decoded Logs Variables #}
  {% set ns.DECODER_SL_DECODED_LOGS_REALTIME_EXTERNAL_TABLE = get_var('DECODER_SL_DECODED_LOGS_REALTIME_EXTERNAL_TABLE', 'decoded_logs') %}
  {% set ns.DECODER_SL_DECODED_LOGS_REALTIME_SQL_LIMIT = get_var('DECODER_SL_DECODED_LOGS_REALTIME_SQL_LIMIT', 10000000) %}
  {% set ns.DECODER_SL_DECODED_LOGS_REALTIME_PRODUCER_BATCH_SIZE = get_var('DECODER_SL_DECODED_LOGS_REALTIME_PRODUCER_BATCH_SIZE', 400000) %}
  {% set ns.DECODER_SL_DECODED_LOGS_REALTIME_WORKER_BATCH_SIZE = get_var('DECODER_SL_DECODED_LOGS_REALTIME_WORKER_BATCH_SIZE', 200000) %}

  {% set ns.DECODER_SL_DECODED_LOGS_HISTORY_EXTERNAL_TABLE = get_var('DECODER_SL_DECODED_LOGS_HISTORY_EXTERNAL_TABLE', 'decoded_logs_history') %}
  {% set ns.DECODER_SL_DECODED_LOGS_HISTORY_SQL_LIMIT = get_var('DECODER_SL_DECODED_LOGS_HISTORY_SQL_LIMIT', 8000000) %}
  {% set ns.DECODER_SL_DECODED_LOGS_HISTORY_PRODUCER_BATCH_SIZE = get_var('DECODER_SL_DECODED_LOGS_HISTORY_PRODUCER_BATCH_SIZE', 400000) %}
  {% set ns.DECODER_SL_DECODED_LOGS_HISTORY_WORKER_BATCH_SIZE = get_var('DECODER_SL_DECODED_LOGS_HISTORY_WORKER_BATCH_SIZE', 100000) %}
  {% set ns.DECODER_SL_DECODED_LOGS_HISTORY_WAIT_SECONDS = get_var('DECODER_SL_DECODED_LOGS_HISTORY_WAIT_SECONDS', 60) %}

  {# ABIs Bronze Variables #}
  {% set ns.DECODER_ABIS_EXPLORER_LIMIT = get_var('DECODER_ABIS_EXPLORER_LIMIT', 50) %}
  {% set ns.DECODER_ABIS_EXPLORER_URL = get_var('DECODER_ABIS_EXPLORER_URL', '') %}
  {% set ns.DECODER_ABIS_EXPLORER_URL_SUFFIX = get_var('DECODER_ABIS_EXPLORER_URL_SUFFIX', '') %}
  {% set ns.DECODER_ABIS_EXPLORER_API_KEY_VAULT_PATH = get_var('DECODER_ABIS_EXPLORER_API_KEY_VAULT_PATH', '') %}
  {% set ns.DECODER_ABIS_EXPLORER_INTERACTION_LIMIT = get_var('DECODER_ABIS_EXPLORER_INTERACTION_LIMIT', 250) %}

  {# ABIs Silver Variables #}
  {% set ns.DECODER_ABIS_EXPLORER_NAME = get_var('DECODER_ABIS_EXPLORER_NAME', '') %}
  {% set ns.DECODER_ABIS_ETHERSCAN_ENABLED = get_var('DECODER_ABIS_ETHERSCAN_ENABLED', false) %}
  {% set ns.DECODER_ABIS_RESULT_OUTPUT_ABI_ENABLED = get_var('DECODER_ABIS_RESULT_OUTPUT_ABI_ENABLED', false) %}

  {# Observability Variables #}
  {% set ns.MAIN_OBSERV_FULL_TEST_ENABLED = get_var('MAIN_OBSERV_FULL_TEST_ENABLED', false) %}
  {% set ns.MAIN_OBSERV_BLOCKS_EXCLUSION_LIST_ENABLED = get_var('MAIN_OBSERV_BLOCKS_EXCLUSION_LIST_ENABLED', false) %}
  {% set ns.MAIN_OBSERV_LOGS_EXCLUSION_LIST_ENABLED = get_var('MAIN_OBSERV_LOGS_EXCLUSION_LIST_ENABLED', false) %}
  {% set ns.MAIN_OBSERV_RECEIPTS_EXCLUSION_LIST_ENABLED = get_var('MAIN_OBSERV_RECEIPTS_EXCLUSION_LIST_ENABLED', false) %}
  {% set ns.MAIN_OBSERV_TRACES_EXCLUSION_LIST_ENABLED = get_var('MAIN_OBSERV_TRACES_EXCLUSION_LIST_ENABLED', false) %}
  {% set ns.MAIN_OBSERV_TRANSACTIONS_EXCLUSION_LIST_ENABLED = get_var('MAIN_OBSERV_TRANSACTIONS_EXCLUSION_LIST_ENABLED', false) %}
  
  {# Prices Variables #}
  {% set ns.MAIN_PRICES_NATIVE_SYMBOLS = get_var('MAIN_PRICES_NATIVE_SYMBOLS', '') %}
  {% set ns.MAIN_PRICES_NATIVE_BLOCKCHAINS = get_var('MAIN_PRICES_NATIVE_BLOCKCHAINS', ns.GLOBAL_PROJECT_NAME.lower()) %}
  {% set ns.MAIN_PRICES_PROVIDER_PLATFORMS = get_var('MAIN_PRICES_PROVIDER_PLATFORMS', '') %}
  {% set ns.MAIN_PRICES_TOKEN_ADDRESSES = get_var('MAIN_PRICES_TOKEN_ADDRESSES', none) %}
  {% set ns.MAIN_PRICES_TOKEN_BLOCKCHAINS = get_var('MAIN_PRICES_TOKEN_BLOCKCHAINS', ns.GLOBAL_PROJECT_NAME.lower()) %}

  {# Labels Variables #}
  {% set ns.MAIN_LABELS_BLOCKCHAINS = get_var('MAIN_LABELS_BLOCKCHAINS', ns.GLOBAL_PROJECT_NAME.lower()) %}

  {# Scores Variables #}
  {% set ns.SCORES_FULL_RELOAD_ENABLED = get_var('SCORES_FULL_RELOAD_ENABLED', false) %}
  {% set ns.SCORES_LIMIT_DAYS = get_var('SCORES_LIMIT_DAYS', 30) %}
  
  {# NFT Variables #}
  {% set ns.MAIN_NFT_TRANSFERS_UNIQUE_KEY = 'tx_hash' if ns.MAIN_CORE_RECEIPTS_BY_HASH_ENABLED else 'block_number' %}

  {# Vertex Variables #}
  {% set ns.CURATED_VERTEX_OFFCHAIN_EXCHANGE_CONTRACT = get_var('CURATED_VERTEX_OFFCHAIN_EXCHANGE_CONTRACT', '') %}
  {% set ns.CURATED_VERTEX_CLEARINGHOUSE_CONTRACT = get_var('CURATED_VERTEX_CLEARINGHOUSE_CONTRACT', '') %}
  {% set ns.CURATED_VERTEX_TOKEN_MAPPING = get_var('CURATED_VERTEX_TOKEN_MAPPING', {}) %}
  
  {# Return the entire namespace as a dictionary #}
  {{ return(ns) }}
{% endmacro %}