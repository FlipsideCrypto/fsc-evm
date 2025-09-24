{% macro streamline_balances_erc20_daily_history_batch_function_call() %}

{# Get variables #}
{% set vars = return_vars() %}

    {# Get the maximum batch number from the history table #}
    {% set max_batch_query %}
        SELECT COALESCE(MAX(batch), -1) as max_batch 
        FROM {{ this.schema }}.{{ this.identifier }}
    {% endset %}
    
    {% if execute %}
        {% set results = run_query(max_batch_query) %}
        {% set max_batch = results.columns[0].values()[0] %}
        
        {# Process each batch sequentially #}
        {% for batch_num in range(max_batch + 1) %}
            {{ log("Processing batch " ~ batch_num ~ " of " ~ max_batch, info=True) }}
            
            {# Create a temporary view for this batch #}
            {% set batch_view_name = this.identifier ~ '_batch_' ~ batch_num %}
            {% set create_batch_view %}
                CREATE OR REPLACE VIEW streamline.{{ batch_view_name }} AS
                SELECT * FROM {{ this.schema }}.{{ this.identifier }}
                WHERE batch = {{ batch_num }}
            {% endset %}
            {% do run_query(create_batch_view) %}
            {{ log("Created view: streamline." ~ batch_view_name, info=True) }}
            
            {% set params = {
                "external_table": 'balances_erc20',
                "sql_limit": vars.BALANCES_SL_DAILY_HISTORY_BATCH_SIZE,
                "producer_batch_size": vars.BALANCES_SL_ERC20_DAILY_HISTORY_PRODUCER_BATCH_SIZE,
                "worker_batch_size": vars.BALANCES_SL_ERC20_DAILY_HISTORY_WORKER_BATCH_SIZE,
                "async_concurrent_requests": vars.BALANCES_SL_ERC20_DAILY_HISTORY_ASYNC_CONCURRENT_REQUESTS,
                "sql_source": 'balances_erc20_daily_history'
            } %}

            {% set function_call_sql %}
            {{ fsc_utils.if_data_call_function_v2(
                func = 'streamline.udf_bulk_rest_api_v2',
                target = "streamline." ~ batch_view_name,
                params = params
            ) }}
            {% endset %}

            {% do run_query(function_call_sql) %}
            {{ log("Completed batch " ~ batch_num, info=True) }}
        {% endfor %}
    {% endif %}

{% endmacro %}

{% macro streamline_balances_native_daily_history_batch_function_call() %}

{# Get variables #}
{% set vars = return_vars() %}

    {# Get the maximum batch number from the history table #}
    {% set max_batch_query %}
        SELECT COALESCE(MAX(batch), -1) AS max_batch 
        FROM {{ this.schema }}.{{ this.identifier }}
    {% endset %}
    
    {% if execute %}
        {% set results = run_query(max_batch_query) %}
        {% set max_batch = results.columns[0].values()[0] %}
        
        {# Process each batch sequentially #}
        {% for batch_num in range(max_batch + 1) %}
            {{ log("Processing batch " ~ batch_num ~ " of " ~ max_batch, info=True) }}
            
            {# Create a temporary view for this batch #}
            {% set batch_view_name = this.identifier ~ '_batch_' ~ batch_num %}
            {% set create_batch_view %}
                CREATE OR REPLACE VIEW streamline.{{ batch_view_name }} AS
                SELECT * FROM {{ this.schema }}.{{ this.identifier }}
                WHERE batch = {{ batch_num }}
            {% endset %}
            {% do run_query(create_batch_view) %}
            {{ log("Created view: streamline." ~ batch_view_name, info=True) }}
            
            {% set params = {
                "external_table": 'balances_native',
                "sql_limit": vars.BALANCES_SL_DAILY_HISTORY_BATCH_SIZE,
                "producer_batch_size": vars.BALANCES_SL_NATIVE_DAILY_HISTORY_PRODUCER_BATCH_SIZE,
                "worker_batch_size": vars.BALANCES_SL_NATIVE_DAILY_HISTORY_WORKER_BATCH_SIZE,
                "async_concurrent_requests": vars.BALANCES_SL_NATIVE_DAILY_HISTORY_ASYNC_CONCURRENT_REQUESTS,
                "sql_source": 'balances_native_daily_history'
            } %}

            {% set function_call_sql %}
            {{ fsc_utils.if_data_call_function_v2(
                func = 'streamline.udf_bulk_rest_api_v2',
                target = "streamline." ~ batch_view_name,
                params = params
            ) }}
            {% endset %}

            {% do run_query(function_call_sql) %}
            {{ log("Completed batch " ~ batch_num, info=True) }}
        {% endfor %}
    {% endif %}

{% endmacro %}