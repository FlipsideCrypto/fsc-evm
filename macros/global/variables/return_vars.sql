{% macro return_vars() %}
  {# This macro sets and returns all configurable variables used throughout the project,
  organizing them BY category (
    global,
    bronze,
    silver,
    streamline,
    decoder etc.
  ) WITH DEFAULT
VALUES.important: ONLY call get_var() once per VARIABLE #}
  {# Set all variables on the namespace #}
  {% set ns = namespace() %}
  {# Set Variables and Default Values, organized by category #}
  {# Global Variables #}
  {% set ns.global_project_name = get_var(
    'GLOBAL_PROJECT_NAME',
    ''
  ) %}
  {% set ns.global_node_provider = get_var(
    'GLOBAL_NODE_PROVIDER',
    ''
  ) %}
  {% set ns.global_node_url = get_var(
    'GLOBAL_NODE_URL',
    '{Service}/{Authentication}'
  ) %}
  {% set ns.global_native_asset_symbol = get_var(
    'GLOBAL_NATIVE_ASSET_SYMBOL',
    ''
  ) %}
  {% set ns.global_wrapped_native_asset_address = get_var(
    'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS',
    ''
  ) %}
  {% set ns.global_wrapped_native_asset_symbol = get_var(
    'GLOBAL_WRAPPED_NATIVE_ASSET_SYMBOL',
    ''
  ) %}
  {% set ns.global_max_sequence_number = get_var(
    'GLOBAL_MAX_SEQUENCE_NUMBER',
    1000000000
  ) %}
  {% set ns.global_node_vault_path = get_var(
    'GLOBAL_NODE_VAULT_PATH',
    ''
  ) %}
  {% set ns.global_network = get_var(
    'GLOBAL_NETWORK',
    'mainnet'
  ) %}
  {% set ns.global_start_block = get_var(
    'GLOBAL_START_BLOCK',
    0
  ) %}
  {% set ns.global_bronze_fr_enabled = none if get_var(
    'GLOBAL_BRONZE_FR_ENABLED',
    false
  ) else false %}
  {# Sets to none if true, still requires --full-refresh, otherwise will use incremental #}
  {% set ns.global_silver_fr_enabled = none if get_var(
    'GLOBAL_SILVER_FR_ENABLED',
    false
  ) else false %}
  {% set ns.global_gold_fr_enabled = none if get_var(
    'GLOBAL_GOLD_FR_ENABLED',
    false
  ) else false %}
  {% set ns.global_streamline_fr_enabled = none if get_var(
    'GLOBAL_STREAMLINE_FR_ENABLED',
    false
  ) else false %}
  {% set ns.global_new_build_enabled = get_var(
    'GLOBAL_NEW_BUILD_ENABLED',
    false
  ) %}
  {% set ns.global_change_tracking_enabled = get_var(
    'GLOBAL_CHANGE_TRACKING_ENABLED',
    false
  ) %}
  {# Main GHA Workflow Variables #}
  {% set ns.main_gha_streamline_chainhead_cron = get_var(
    'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON',
    '0,30 * * * *'
  ) %}
  {% set ns.main_gha_scheduled_main_cron = get_var(
    'MAIN_GHA_SCHEDULED_MAIN_CRON',
    none
  ) %}
  {% set ns.main_gha_scheduled_curated_cron = get_var(
    'MAIN_GHA_SCHEDULED_CURATED_CRON',
    none
  ) %}
  {% set ns.main_gha_scheduled_abis_cron = get_var(
    'MAIN_GHA_SCHEDULED_ABIS_CRON',
    none
  ) %}
  {% set ns.main_gha_scheduled_scores_cron = get_var(
    'MAIN_GHA_SCHEDULED_SCORES_CRON',
    none
  ) %}
  {% set ns.main_gha_test_daily_cron = get_var(
    'MAIN_GHA_TEST_DAILY_CRON',
    none
  ) %}
  {% set ns.main_gha_test_intraday_cron = get_var(
    'MAIN_GHA_TEST_INTRADAY_CRON',
    none
  ) %}
  {% set ns.main_gha_test_monthly_cron = get_var(
    'MAIN_GHA_TEST_MONTHLY_CRON',
    none
  ) %}
  {% set ns.main_gha_heal_models_cron = get_var(
    'MAIN_GHA_HEAL_MODELS_CRON',
    none
  ) %}
  {% set ns.main_gha_full_observability_cron = get_var(
    'MAIN_GHA_FULL_OBSERVABILITY_CRON',
    none
  ) %}
  {% set ns.main_gha_dev_refresh_cron = get_var(
    'MAIN_GHA_DEV_REFRESH_CRON',
    none
  ) %}
  {% set ns.main_gha_streamline_decoder_history_cron = get_var(
    'MAIN_GHA_STREAMLINE_DECODER_HISTORY_CRON',
    none
  ) %}
  {# Custom GHA Workflow Variables #}
  {% set ns.custom_gha_streamline_dexalot_chainhead_cron = get_var(
    'CUSTOM_GHA_STREAMLINE_DEXALOT_CHAINHEAD_CRON',
    none
  ) %}
  {% set ns.custom_gha_scheduled_dexalot_main_cron = get_var(
    'CUSTOM_GHA_SCHEDULED_DEXALOT_MAIN_CRON',
    none
  ) %}
  {% set ns.custom_gha_test_beacon_cron = get_var(
    'CUSTOM_GHA_TEST_BEACON_CRON',
    none
  ) %}
  {% set ns.custom_gha_streamline_reads_cron = get_var(
    'CUSTOM_GHA_STREAMLINE_READS_CRON',
    none
  ) %}
  {% set ns.custom_gha_streamline_beacon_cron = get_var(
    'CUSTOM_GHA_STREAMLINE_BEACON_CRON',
    none
  ) %}
  {% set ns.custom_gha_scheduled_beacon_cron = get_var(
    'CUSTOM_GHA_SCHEDULED_BEACON_CRON',
    none
  ) %}
  {% set ns.custom_gha_nft_reads_cron = get_var(
    'CUSTOM_GHA_NFT_READS_CRON',
    none
  ) %}
  {% set ns.custom_gha_nft_list_cron = get_var(
    'CUSTOM_GHA_NFT_LIST_CRON',
    none
  ) %}
  {# Core Variables #}
  {% set ns.main_core_receipts_by_hash_enabled = get_var(
    'MAIN_CORE_RECEIPTS_BY_HASH_ENABLED',
    false
  ) %}
  {% set ns.main_core_traces_arb_mode = ns.global_project_name.upper() == 'ARBITRUM' %}
  {% set ns.main_core_traces_sei_mode = ns.global_project_name.upper() == 'SEI' %}
  {% set ns.main_core_traces_kaia_mode = ns.global_project_name.upper() == 'KAIA' %}
  {# Core Bronze Variables #}
  {% set ns.main_core_bronze_token_reads_limit = get_var(
    'MAIN_CORE_BRONZE_TOKEN_READS_LIMIT',
    50
  ) %}
  {% set ns.main_core_bronze_token_reads_batched_enabled = get_var(
    'MAIN_CORE_BRONZE_TOKEN_READS_BATCHED_ENABLED',
    false
  ) %}
  {# Core Silver Variables #}
  {% set ns.main_core_silver_receipts_unique_key = 'tx_hash' if ns.main_core_receipts_by_hash_enabled else 'block_number' %}
  {% set ns.main_core_silver_receipts_source_name = 'RECEIPTS_BY_HASH' if ns.main_core_receipts_by_hash_enabled else 'RECEIPTS' %}
  {% set ns.main_core_silver_receipts_post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(tx_hash, block_number)" if ns.main_core_receipts_by_hash_enabled else "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(array_index, block_number)" %}
  {% set ns.main_core_silver_confirm_blocks_full_reload_enabled = get_var(
    'MAIN_CORE_SILVER_CONFIRM_BLOCKS_FULL_RELOAD_ENABLED',
    false
  ) %}
  {% set ns.main_core_silver_traces_full_reload_enabled = get_var(
    'MAIN_CORE_SILVER_TRACES_FULL_RELOAD_ENABLED',
    false
  ) %}
  {% set ns.main_core_silver_traces_fr_max_block = get_var(
    'MAIN_CORE_SILVER_TRACES_FR_MAX_BLOCK',
    1000000
  ) %}
  {% set ns.main_core_silver_traces_full_reload_blocks_per_run = get_var(
    'MAIN_CORE_SILVER_TRACES_FULL_RELOAD_BLOCKS_PER_RUN',
    1000000
  ) %}
  {% set ns.main_core_silver_traces_partition_key_enabled = get_var(
    'MAIN_CORE_SILVER_TRACES_PARTITION_KEY_ENABLED',
    true
  ) %}
  {# Core Gold Variables #}
  {% set ns.main_core_gold_fact_transactions_unique_key = 'tx_hash' if ns.main_core_receipts_by_hash_enabled else 'block_number' %}
  {% set ns.main_core_gold_ez_native_transfers_unique_key = 'tx_hash' if ns.main_core_receipts_by_hash_enabled else 'block_number' %}
  {% set ns.main_core_gold_ez_native_transfers_prices_start_date = get_var(
    'MAIN_CORE_GOLD_EZ_NATIVE_TRANSFERS_PRICES_START_DATE',
    '2024-01-01'
  ) %}
  {% set ns.main_core_gold_fact_event_logs_unique_key = 'tx_hash' if ns.main_core_receipts_by_hash_enabled else 'block_number' %}
  {% set ns.main_core_gold_traces_full_reload_enabled = get_var(
    'MAIN_CORE_GOLD_TRACES_FULL_RELOAD_ENABLED',
    false
  ) %}
  {% set ns.main_core_gold_traces_fr_max_block = get_var(
    'MAIN_CORE_GOLD_TRACES_FR_MAX_BLOCK',
    1000000
  ) %}
  {% set ns.main_core_gold_traces_full_reload_blocks_per_run = get_var(
    'MAIN_CORE_GOLD_TRACES_FULL_RELOAD_BLOCKS_PER_RUN',
    1000000
  ) %}
  {% set ns.main_core_gold_traces_tx_status_enabled = get_var(
    'MAIN_CORE_GOLD_TRACES_TX_STATUS_ENABLED',
    false
  ) %}
  {% set ns.main_core_gold_traces_schema_name = get_var(
    'MAIN_CORE_GOLD_TRACES_SCHEMA_NAME',
    'silver'
  ) %}
  {% if ns.main_core_receipts_by_hash_enabled %}
    {% if ns.main_core_traces_sei_mode %}
      {% set ns.main_core_gold_traces_unique_key = "concat(block_number, '-', tx_hash)" %}
    {% else %}
      {% set ns.main_core_gold_traces_unique_key = "concat(block_number, '-', tx_position)" %}
    {% endif %}
  {% else %}
    {% set ns.main_core_gold_traces_unique_key = "block_number" %}
  {% endif %}

  {# Main Streamline Variables #}
  {% set ns.main_sl_blocks_per_hour = get_var(
    'MAIN_SL_BLOCKS_PER_HOUR',
    1
  ) %}
  {% set ns.main_sl_transactions_per_block = get_var(
    'MAIN_SL_TRANSACTIONS_PER_BLOCK',
    1
  ) %}
  {% set ns.main_sl_testing_limit = get_var(
    'MAIN_SL_TESTING_LIMIT',
    none
  ) %}
  {% set ns.main_sl_new_build_enabled = get_var(
    'MAIN_SL_NEW_BUILD_ENABLED',
    false
  ) %}
  {% set ns.main_sl_chainhead_delay_minutes = get_var(
    'MAIN_SL_CHAINHEAD_DELAY_MINUTES',
    3
  ) %}
  {% set ns.main_sl_block_lookback_enabled = get_var(
    'MAIN_SL_BLOCK_LOOKBACK_ENABLED',
    true
  ) %}
  {# Main Test Variables #}
  {% set ns.main_core_gold_traces_test_error_threshold = get_var(
    'MAIN_CORE_GOLD_TRACES_TEST_ERROR_THRESHOLD',
    0
  ) %}
  {# SL Blocks Transactions Variables #}
  {% set ns.main_sl_blocks_transactions_realtime_sql_limit = get_var(
    'MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_SQL_LIMIT',
    2 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.main_sl_blocks_transactions_realtime_producer_batch_size = get_var(
    'MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_PRODUCER_BATCH_SIZE',
    2 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.main_sl_blocks_transactions_realtime_worker_batch_size = get_var(
    'MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_WORKER_BATCH_SIZE',
    ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.main_sl_blocks_transactions_realtime_async_concurrent_requests = get_var(
    'MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_ASYNC_CONCURRENT_REQUESTS',
    100
  ) %}
  {% set ns.main_sl_blocks_transactions_history_sql_limit = get_var(
    'MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_SQL_LIMIT',
    1000 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.main_sl_blocks_transactions_history_producer_batch_size = get_var(
    'MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_PRODUCER_BATCH_SIZE',
    10 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.main_sl_blocks_transactions_history_worker_batch_size = get_var(
    'MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_WORKER_BATCH_SIZE',
    ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.main_sl_blocks_transactions_history_async_concurrent_requests = get_var(
    'MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_ASYNC_CONCURRENT_REQUESTS',
    10
  ) %}
  {# SL Receipts Variables #}
  {% set ns.main_sl_receipts_realtime_sql_limit = get_var(
    'MAIN_SL_RECEIPTS_REALTIME_SQL_LIMIT',
    2 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.main_sl_receipts_realtime_producer_batch_size = get_var(
    'MAIN_SL_RECEIPTS_REALTIME_PRODUCER_BATCH_SIZE',
    2 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.main_sl_receipts_realtime_worker_batch_size = get_var(
    'MAIN_SL_RECEIPTS_REALTIME_WORKER_BATCH_SIZE',
    ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.main_sl_receipts_realtime_async_concurrent_requests = get_var(
    'MAIN_SL_RECEIPTS_REALTIME_ASYNC_CONCURRENT_REQUESTS',
    100
  ) %}
  {% set ns.main_sl_receipts_history_sql_limit = get_var(
    'MAIN_SL_RECEIPTS_HISTORY_SQL_LIMIT',
    1000 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.main_sl_receipts_history_producer_batch_size = get_var(
    'MAIN_SL_RECEIPTS_HISTORY_PRODUCER_BATCH_SIZE',
    10 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.main_sl_receipts_history_worker_batch_size = get_var(
    'MAIN_SL_RECEIPTS_HISTORY_WORKER_BATCH_SIZE',
    ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.main_sl_receipts_history_async_concurrent_requests = get_var(
    'MAIN_SL_RECEIPTS_HISTORY_ASYNC_CONCURRENT_REQUESTS',
    10
  ) %}
  {# SL Receipts By Hash Variables #}
  {% set ns.main_sl_receipts_by_hash_realtime_sql_limit = get_var(
    'MAIN_SL_RECEIPTS_BY_HASH_REALTIME_SQL_LIMIT',
    2 * ns.main_sl_blocks_per_hour * ns.main_sl_transactions_per_block
  ) %}
  {% set ns.main_sl_receipts_by_hash_realtime_producer_batch_size = get_var(
    'MAIN_SL_RECEIPTS_BY_HASH_REALTIME_PRODUCER_BATCH_SIZE',
    2 * ns.main_sl_blocks_per_hour * ns.main_sl_transactions_per_block
  ) %}
  {% set ns.main_sl_receipts_by_hash_realtime_worker_batch_size = get_var(
    'MAIN_SL_RECEIPTS_BY_HASH_REALTIME_WORKER_BATCH_SIZE',
    ns.main_sl_blocks_per_hour * ns.main_sl_transactions_per_block
  ) %}
  {% set ns.main_sl_receipts_by_hash_realtime_async_concurrent_requests = get_var(
    'MAIN_SL_RECEIPTS_BY_HASH_REALTIME_ASYNC_CONCURRENT_REQUESTS',
    100
  ) %}
  {% set ns.main_sl_receipts_by_hash_realtime_txns_model_enabled = get_var(
    'MAIN_SL_RECEIPTS_BY_HASH_REALTIME_TXNS_MODEL_ENABLED',
    true
  ) %}
  {% set ns.main_sl_receipts_by_hash_history_sql_limit = get_var(
    'MAIN_SL_RECEIPTS_BY_HASH_HISTORY_SQL_LIMIT',
    1000 * ns.main_sl_blocks_per_hour * ns.main_sl_transactions_per_block
  ) %}
  {% set ns.main_sl_receipts_by_hash_history_producer_batch_size = get_var(
    'MAIN_SL_RECEIPTS_BY_HASH_HISTORY_PRODUCER_BATCH_SIZE',
    10 * ns.main_sl_blocks_per_hour * ns.main_sl_transactions_per_block
  ) %}
  {% set ns.main_sl_receipts_by_hash_history_worker_batch_size = get_var(
    'MAIN_SL_RECEIPTS_BY_HASH_HISTORY_WORKER_BATCH_SIZE',
    ns.main_sl_blocks_per_hour * ns.main_sl_transactions_per_block
  ) %}
  {% set ns.main_sl_receipts_by_hash_history_async_concurrent_requests = get_var(
    'MAIN_SL_RECEIPTS_BY_HASH_HISTORY_ASYNC_CONCURRENT_REQUESTS',
    10
  ) %}
  {# SL Traces Variables #}
  {% set ns.main_sl_traces_realtime_sql_limit = get_var(
    'MAIN_SL_TRACES_REALTIME_SQL_LIMIT',
    2 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.main_sl_traces_realtime_producer_batch_size = get_var(
    'MAIN_SL_TRACES_REALTIME_PRODUCER_BATCH_SIZE',
    2 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.main_sl_traces_realtime_worker_batch_size = get_var(
    'MAIN_SL_TRACES_REALTIME_WORKER_BATCH_SIZE',
    ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.main_sl_traces_realtime_async_concurrent_requests = get_var(
    'MAIN_SL_TRACES_REALTIME_ASYNC_CONCURRENT_REQUESTS',
    100
  ) %}
  {% set ns.main_sl_traces_realtime_request_start_block = get_var(
    'MAIN_SL_TRACES_REALTIME_REQUEST_START_BLOCK',
    none
  ) %}
  {% set ns.main_sl_traces_history_sql_limit = get_var(
    'MAIN_SL_TRACES_HISTORY_SQL_LIMIT',
    1000 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.main_sl_traces_history_producer_batch_size = get_var(
    'MAIN_SL_TRACES_HISTORY_PRODUCER_BATCH_SIZE',
    10 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.main_sl_traces_history_worker_batch_size = get_var(
    'MAIN_SL_TRACES_HISTORY_WORKER_BATCH_SIZE',
    ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.main_sl_traces_history_async_concurrent_requests = get_var(
    'MAIN_SL_TRACES_HISTORY_ASYNC_CONCURRENT_REQUESTS',
    10
  ) %}
  {% set ns.main_sl_traces_history_request_start_block = get_var(
    'MAIN_SL_TRACES_HISTORY_REQUEST_START_BLOCK',
    none
  ) %}
  {# SL Confirm Blocks Variables #}
  {% set ns.main_sl_confirm_blocks_realtime_sql_limit = get_var(
    'MAIN_SL_CONFIRM_BLOCKS_REALTIME_SQL_LIMIT',
    2 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.main_sl_confirm_blocks_realtime_producer_batch_size = get_var(
    'MAIN_SL_CONFIRM_BLOCKS_REALTIME_PRODUCER_BATCH_SIZE',
    2 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.main_sl_confirm_blocks_realtime_worker_batch_size = get_var(
    'MAIN_SL_CONFIRM_BLOCKS_REALTIME_WORKER_BATCH_SIZE',
    ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.main_sl_confirm_blocks_realtime_async_concurrent_requests = get_var(
    'MAIN_SL_CONFIRM_BLOCKS_REALTIME_ASYNC_CONCURRENT_REQUESTS',
    100
  ) %}
  {% set ns.main_sl_confirm_blocks_history_sql_limit = get_var(
    'MAIN_SL_CONFIRM_BLOCKS_HISTORY_SQL_LIMIT',
    1000 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.main_sl_confirm_blocks_history_producer_batch_size = get_var(
    'MAIN_SL_CONFIRM_BLOCKS_HISTORY_PRODUCER_BATCH_SIZE',
    10 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.main_sl_confirm_blocks_history_worker_batch_size = get_var(
    'MAIN_SL_CONFIRM_BLOCKS_HISTORY_WORKER_BATCH_SIZE',
    ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.main_sl_confirm_blocks_history_async_concurrent_requests = get_var(
    'MAIN_SL_CONFIRM_BLOCKS_HISTORY_ASYNC_CONCURRENT_REQUESTS',
    10
  ) %}
  {# SL Decoder Variables #}
  {% set ns.decoder_sl_testing_limit = get_var(
    'DECODER_SL_TESTING_LIMIT',
    none
  ) %}
  {% set ns.decoder_sl_new_build_enabled = get_var(
    'DECODER_SL_NEW_BUILD_ENABLED',
    false
  ) %}
  {# SL Decoded Logs Variables #}
  {% set ns.decoder_sl_decoded_logs_realtime_external_table = get_var(
    'DECODER_SL_DECODED_LOGS_REALTIME_EXTERNAL_TABLE',
    'decoded_logs'
  ) %}
  {% set ns.decoder_sl_decoded_logs_realtime_sql_limit = get_var(
    'DECODER_SL_DECODED_LOGS_REALTIME_SQL_LIMIT',
    10000000
  ) %}
  {% set ns.decoder_sl_decoded_logs_realtime_producer_batch_size = get_var(
    'DECODER_SL_DECODED_LOGS_REALTIME_PRODUCER_BATCH_SIZE',
    5000000
  ) %}
  {% set ns.decoder_sl_decoded_logs_realtime_worker_batch_size = get_var(
    'DECODER_SL_DECODED_LOGS_REALTIME_WORKER_BATCH_SIZE',
    50000
  ) %}
  {% set ns.decoder_sl_decoded_logs_history_external_table = get_var(
    'DECODER_SL_DECODED_LOGS_HISTORY_EXTERNAL_TABLE',
    'decoded_logs_history'
  ) %}
  {% set ns.decoder_sl_decoded_logs_history_sql_limit = get_var(
    'DECODER_SL_DECODED_LOGS_HISTORY_SQL_LIMIT',
    10000000
  ) %}
  {% set ns.decoder_sl_decoded_logs_history_producer_batch_size = get_var(
    'DECODER_SL_DECODED_LOGS_HISTORY_PRODUCER_BATCH_SIZE',
    5000000
  ) %}
  {% set ns.decoder_sl_decoded_logs_history_worker_batch_size = get_var(
    'DECODER_SL_DECODED_LOGS_HISTORY_WORKER_BATCH_SIZE',
    50000
  ) %}
  {% set ns.decoder_sl_decoded_logs_history_wait_seconds = get_var(
    'DECODER_SL_DECODED_LOGS_HISTORY_WAIT_SECONDS',
    60
  ) %}
  {# SL Contract ABIs Variables #}
  {% set ns.decoder_sl_contract_abis_realtime_sql_limit = get_var(
    'DECODER_SL_CONTRACT_ABIS_REALTIME_SQL_LIMIT',
    100
  ) %}
  {% set ns.decoder_sl_contract_abis_realtime_producer_batch_size = get_var(
    'DECODER_SL_CONTRACT_ABIS_REALTIME_PRODUCER_BATCH_SIZE',
    1
  ) %}
  {% set ns.decoder_sl_contract_abis_realtime_worker_batch_size = get_var(
    'DECODER_SL_CONTRACT_ABIS_REALTIME_WORKER_BATCH_SIZE',
    1
  ) %}
  {% set ns.decoder_sl_contract_abis_realtime_async_concurrent_requests = get_var(
    'DECODER_SL_CONTRACT_ABIS_REALTIME_ASYNC_CONCURRENT_REQUESTS',
    1
  ) %}
  {% set ns.decoder_sl_contract_abis_interaction_count = get_var(
    'DECODER_SL_CONTRACT_ABIS_INTERACTION_COUNT',
    50
  ) %}
  {% set ns.decoder_sl_contract_abis_explorer_url = get_var(
    'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL',
    ''
  ) %}
  {% set ns.decoder_sl_contract_abis_explorer_url_suffix = get_var(
    'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL_SUFFIX',
    ''
  ) %}
  {% set ns.decoder_sl_contract_abis_explorer_vault_path = get_var(
    'DECODER_SL_CONTRACT_ABIS_EXPLORER_VAULT_PATH',
    ''
  ) %}
  {% set ns.decoder_sl_contract_abis_bronze_table_enabled = get_var(
    'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED',
    false
  ) %}
  {# ABIs Silver Variables #}
  {% set ns.decoder_silver_contract_abis_explorer_name = get_var(
    'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME',
    ''
  ) %}
  {% set ns.decoder_silver_contract_abis_etherscan_enabled = get_var(
    'DECODER_SILVER_CONTRACT_ABIS_ETHERSCAN_ENABLED',
    false
  ) %}
  {% set ns.decoder_silver_contract_abis_result_enabled = get_var(
    'DECODER_SILVER_CONTRACT_ABIS_RESULT_ENABLED',
    false
  ) %}
  {# SL Balances Variables #}
  {% set ns.balances_sl_testing_limit = get_var(
    'BALANCES_SL_TESTING_LIMIT',
    none
  ) %}
  {% set ns.balances_sl_new_build_enabled = get_var(
    'BALANCES_SL_NEW_BUILD_ENABLED',
    false
  ) %}
  {% set ns.balances_sl_start_date = get_var(
    'BALANCES_SL_START_DATE',
    '2025-06-10'
  ) %}
  {% set ns.balances_sl_erc20_daily_realtime_sql_limit = get_var(
    'BALANCES_SL_ERC20_DAILY_REALTIME_SQL_LIMIT',
    5000 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.balances_sl_erc20_daily_realtime_producer_batch_size = get_var(
    'BALANCES_SL_ERC20_DAILY_REALTIME_PRODUCER_BATCH_SIZE',
    500 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.balances_sl_erc20_daily_realtime_worker_batch_size = get_var(
    'BALANCES_SL_ERC20_DAILY_REALTIME_WORKER_BATCH_SIZE',
    35 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.balances_sl_erc20_daily_realtime_async_concurrent_requests = get_var(
    'BALANCES_SL_ERC20_DAILY_REALTIME_ASYNC_CONCURRENT_REQUESTS',
    20
  ) %}
  {% set ns.balances_sl_erc20_daily_history_sql_limit = get_var(
    'BALANCES_SL_ERC20_DAILY_HISTORY_SQL_LIMIT',
    10000 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.balances_sl_erc20_daily_history_producer_batch_size = get_var(
    'BALANCES_SL_ERC20_DAILY_HISTORY_PRODUCER_BATCH_SIZE',
    500 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.balances_sl_erc20_daily_history_worker_batch_size = get_var(
    'BALANCES_SL_ERC20_DAILY_HISTORY_WORKER_BATCH_SIZE',
    35 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.balances_sl_erc20_daily_history_async_concurrent_requests = get_var(
    'BALANCES_SL_ERC20_DAILY_HISTORY_ASYNC_CONCURRENT_REQUESTS',
    20
  ) %}
  {% set ns.balances_sl_native_daily_realtime_sql_limit = get_var(
    'BALANCES_SL_NATIVE_DAILY_REALTIME_SQL_LIMIT',
    5000 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.balances_sl_native_daily_realtime_producer_batch_size = get_var(
    'BALANCES_SL_NATIVE_DAILY_REALTIME_PRODUCER_BATCH_SIZE',
    500 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.balances_sl_native_daily_realtime_worker_batch_size = get_var(
    'BALANCES_SL_NATIVE_DAILY_REALTIME_WORKER_BATCH_SIZE',
    35 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.balances_sl_native_daily_realtime_async_concurrent_requests = get_var(
    'BALANCES_SL_NATIVE_DAILY_REALTIME_ASYNC_CONCURRENT_REQUESTS',
    20
  ) %}
  {% set ns.balances_sl_native_daily_history_sql_limit = get_var(
    'BALANCES_SL_NATIVE_DAILY_HISTORY_SQL_LIMIT',
    10000 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.balances_sl_native_daily_history_producer_batch_size = get_var(
    'BALANCES_SL_NATIVE_DAILY_HISTORY_PRODUCER_BATCH_SIZE',
    500 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.balances_sl_native_daily_history_worker_batch_size = get_var(
    'BALANCES_SL_NATIVE_DAILY_HISTORY_WORKER_BATCH_SIZE',
    35 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.balances_sl_native_daily_history_async_concurrent_requests = get_var(
    'BALANCES_SL_NATIVE_DAILY_HISTORY_ASYNC_CONCURRENT_REQUESTS',
    20
  ) %}
  {% set ns.balances_sl_state_tracer_realtime_sql_limit = get_var(
    'BALANCES_SL_STATE_TRACER_REALTIME_SQL_LIMIT',
    2 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.balances_sl_state_tracer_realtime_producer_batch_size = get_var(
    'BALANCES_SL_STATE_TRACER_REALTIME_PRODUCER_BATCH_SIZE',
    2 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.balances_sl_state_tracer_realtime_worker_batch_size = get_var(
    'BALANCES_SL_STATE_TRACER_REALTIME_WORKER_BATCH_SIZE',
    ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.balances_sl_state_tracer_realtime_async_concurrent_requests = get_var(
    'BALANCES_SL_STATE_TRACER_REALTIME_ASYNC_CONCURRENT_REQUESTS',
    100
  ) %}
  {% set ns.balances_sl_state_tracer_history_sql_limit = get_var(
    'BALANCES_SL_STATE_TRACER_HISTORY_SQL_LIMIT',
    1000 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.balances_sl_state_tracer_history_producer_batch_size = get_var(
    'BALANCES_SL_STATE_TRACER_HISTORY_PRODUCER_BATCH_SIZE',
    10 * ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.balances_sl_state_tracer_history_worker_batch_size = get_var(
    'BALANCES_SL_STATE_TRACER_HISTORY_WORKER_BATCH_SIZE',
    ns.main_sl_blocks_per_hour
  ) %}
  {% set ns.balances_sl_state_tracer_history_async_concurrent_requests = get_var(
    'BALANCES_SL_STATE_TRACER_HISTORY_ASYNC_CONCURRENT_REQUESTS',
    10
  ) %}
  {# Balances State Tracer Silver Variables #}
  {% set ns.balances_exclusion_list_enabled = get_var(
    'BALANCES_EXCLUSION_LIST_ENABLED',
    false
  ) %}
  {% set ns.balances_validator_contract_address = get_var(
    'BALANCES_VALIDATOR_CONTRACT_ADDRESS',
    ''
  ) %}
  {% set ns.balances_silver_state_tracer_full_reload_enabled = get_var(
    'BALANCES_SILVER_STATE_TRACER_FULL_RELOAD_ENABLED',
    false
  ) %}
  {% set ns.balances_silver_state_tracer_fr_max_block = get_var(
    'BALANCES_SILVER_STATE_TRACER_FR_MAX_BLOCK',
    800000
  ) %}
  {% set ns.balances_silver_state_tracer_full_reload_blocks_per_run = get_var(
    'BALANCES_SILVER_STATE_TRACER_FULL_RELOAD_BLOCKS_PER_RUN',
    800000
  ) %}
  {# SL Token Reads Variables #}
  {% set ns.main_sl_token_reads_bronze_table_enabled = get_var(
    'MAIN_SL_TOKEN_READS_BRONZE_TABLE_ENABLED',
    false
  ) %}
  {% set ns.main_sl_token_reads_contract_limit = get_var(
    'MAIN_SL_TOKEN_READS_CONTRACT_LIMIT',
    1000
  ) %}
  {% set ns.main_sl_token_reads_realtime_sql_limit = get_var(
    'MAIN_SL_TOKEN_READS_REALTIME_SQL_LIMIT',
    3000
  ) %}
  {% set ns.main_sl_token_reads_realtime_producer_batch_size = get_var(
    'MAIN_SL_TOKEN_READS_REALTIME_PRODUCER_BATCH_SIZE',
    1500
  ) %}
  {% set ns.main_sl_token_reads_realtime_worker_batch_size = get_var(
    'MAIN_SL_TOKEN_READS_REALTIME_WORKER_BATCH_SIZE',
    500
  ) %}
  {% set ns.main_sl_token_reads_realtime_async_concurrent_requests = get_var(
    'MAIN_SL_TOKEN_READS_REALTIME_ASYNC_CONCURRENT_REQUESTS',
    5
  ) %}
  {# Observability Variables #}
  {% set ns.main_observ_full_test_enabled = get_var(
    'MAIN_OBSERV_FULL_TEST_ENABLED',
    false
  ) %}
  {% set ns.main_observ_exclusion_list_enabled = get_var(
    'MAIN_OBSERV_EXCLUSION_LIST_ENABLED',
    false
  ) %}
  {# Prices Variables #}
  {% set ns.main_prices_native_symbols = get_var(
    'MAIN_PRICES_NATIVE_SYMBOLS',
    ''
  ) %}
  {% set ns.main_prices_native_blockchains = get_var(
    'MAIN_PRICES_NATIVE_BLOCKCHAINS',
    ns.global_project_name.lower()
  ) %}
  {% set ns.main_prices_provider_platforms = get_var(
    'MAIN_PRICES_PROVIDER_PLATFORMS',
    ''
  ) %}
  {% set ns.main_prices_token_addresses = get_var(
    'MAIN_PRICES_TOKEN_ADDRESSES',
    none
  ) %}
  {% set ns.main_prices_token_blockchains = get_var(
    'MAIN_PRICES_TOKEN_BLOCKCHAINS',
    ns.global_project_name.lower()
  ) %}
  {# Labels Variables #}
  {% set ns.main_labels_blockchains = get_var(
    'MAIN_LABELS_BLOCKCHAINS',
    ns.global_project_name.lower()
  ) %}
  {# Scores Variables #}
  {% set ns.scores_full_reload_enabled = get_var(
    'SCORES_FULL_RELOAD_ENABLED',
    false
  ) %}
  {% set ns.scores_limit_days = get_var(
    'SCORES_LIMIT_DAYS',
    30
  ) %}
  {# NFT Variables #}
  {# Curated Variables #}
  {% set ns.curated_complete_lookback_hours = get_var(
    'CURATED_COMPLETE_LOOKBACK_HOURS',
    '4 hours'
  ) %}
  {% set ns.curated_lookback_hours = get_var(
    'CURATED_LOOKBACK_HOURS',
    '12 hours'
  ) %}
  {% set ns.curated_lookback_days = get_var(
    'CURATED_LOOKBACK_DAYS',
    '7 days'
  ) %}
  {% set ns.curated_fr_models = get_var(
    'CURATED_FR_MODELS',
    []
  ) %}
  {% set ns.curated_defi_recency_exclusion_list = get_var(
    'CURATED_DEFI_RECENCY_EXCLUSION_LIST',
    []
  ) %}
  {# Curated Bridge Variables #}
  {% set ns.curated_defi_bridge_contract_mapping = get_var(
    'CURATED_DEFI_BRIDGE_CONTRACT_MAPPING',{}
  ) %}
  {% set ns.curated_defi_bridge_allbridge_source_chain = get_var(
    'CURATED_DEFI_BRIDGE_ALLBRIDGE_SOURCE_CHAIN',
    ''
  ) %}
  {% set ns.curated_defi_bridge_hop_l1_contracts = get_var(
    'CURATED_DEFI_BRIDGE_HOP_L1_CONTRACTS',
    []
  ) %}
  {% set ns.curated_defi_bridge_hop_bridge_contract = get_var(
    'CURATED_DEFI_BRIDGE_HOP_BRIDGE_CONTRACT',
    ''
  ) %}
  {% set ns.curated_defi_bridge_hop_token_contract = get_var(
    'CURATED_DEFI_BRIDGE_HOP_TOKEN_CONTRACT',
    ''
  ) %}
  {% set ns.curated_defi_bridge_locked_contracts = get_var(
    'CURATED_DEFI_BRIDGE_LOCKED_CONTRACTS',
    []
  ) %}
  {# Curated DEX Variables #}
  {% set ns.curated_defi_dex_swaps_contract_mapping = get_var(
    'CURATED_DEFI_DEX_SWAPS_CONTRACT_MAPPING',{}
  ) %}
  {% set ns.curated_defi_dex_dexalot_dest_chain_id = get_var(
    'CURATED_DEFI_DEX_DEXALOT_DEST_CHAIN_ID',
    0
  ) %}
  {# Curated Lending Variables #}
  {% set ns.curated_defi_lending_contract_mapping = get_var(
    'CURATED_DEFI_LENDING_CONTRACT_MAPPING',{}
  ) %}
  {# Return the entire namespace as a dictionary #}
  {{ return(ns) }}
{% endmacro %}
