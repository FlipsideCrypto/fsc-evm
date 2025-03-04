{% macro get_var(variable_key, default=none) %}
    {# Check if variable exists in dbt's built-in var() function. If it does, return the value. #}
    {% if var(variable_key, none) is not none %}
        {{ return(var(variable_key)) }}
    {% endif %}
    {# Query to get variable values from custom variables table #}
    {% set query %}
        SELECT 
            index,
            package,
            category,
            data_type,
            parent_key,
            key,
            VALUE,
            is_enabled
        FROM {{ ref('silver__ez_variables') }}
        WHERE (key = '{{ variable_key }}'
           OR parent_key = '{{ variable_key }}')
           AND is_enabled
        ORDER BY key
    {% endset %}
    
    {% if execute %}
        {% set results = run_query(query) %}
        
        {# If no results found, return the default value #}
        {% if results.rows | length == 0 %}
            {{ return(default) }}
        {% endif %}
        {% set data_type = results.rows[0][3].lower() %}
        {% set parent_key = results.rows[0][4] %}
        {% set value = results.rows[0][6] %}
        {% set is_enabled = results.rows[0][7] %}
        
        {# Check if this is a simple variable (no parent key) or a mapping (has parent key) #}
        {% if parent_key is none or parent_key == '' %}
            {% if data_type == 'array' %}
                {# For array type, parse and convert values to appropriate types #}
                {% set array_values = value.split(',') %}
                {% set converted_array = [] %}
                {% for val in array_values %}
                    {% set stripped_val = val.strip() %}
                    {% if stripped_val.isdigit() %}
                        {% do converted_array.append(stripped_val | int) %}
                    {% elif stripped_val.replace('.','',1).isdigit() %}
                        {% do converted_array.append(stripped_val | float) %}
                    {% elif stripped_val.lower() in ['true', 'false'] %}
                        {% do converted_array.append(stripped_val.lower() == 'true') %}
                    {% else %}
                        {% do converted_array.append(stripped_val) %}
                    {% endif %}
                {% endfor %}
                {{ return(converted_array) }}
            {# For other types, convert appropriately #}
            {% elif data_type in ['json', 'variant', 'object'] %}
                {{ return(fromjson(value)) }}
            {% elif data_type in ['number', 'integer', 'fixed', 'float', 'decimal'] %}
                {% if '.' in value %}
                    {{ return(value | float) }}
                {% else %}
                    {{ return(value | int) }}
                {% endif %}
            {% elif data_type == 'boolean' %}
                {{ return(value | lower == 'true') }}
            {% else %}
                {{ return(value) }}
            {% endif %}
        {% else %}
            {# For variables with a parent_key, build a dictionary of all child values #}
            {% set mapping = {} %}
            {% for row in results.rows %}
                {# key: value pairings based on parent_key #}
                {% do mapping.update({row[5]: row[6]}) %} 
            {% endfor %}
            {{ return(mapping) }}
        {% endif %}
    {% else %}
        {{ return(default) }}
    {% endif %}
{% endmacro %}


{% macro return_vars() %}
  {# Set all variables on the namespace #}
  {% set ns = namespace() %}

  {# Global Variables #}
  {% set ns.node_url = get_var('GLOBAL_NODE_URL', '{Service}/{Authentication}') %}
  {% set ns.node_secret_path = get_var('GLOBAL_NODE_VAULT_PATH', '') %}
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
  {% set ns.MAIN_PRICES_NATIVE_BLOCKCHAINS = get_var('MAIN_PRICES_NATIVE_BLOCKCHAINS', get_var('GLOBAL_PROD_DB_NAME', '').lower()) %}

  {# Return the entire namespace as a dictionary #}
  {{ return(ns) }}
{% endmacro %}