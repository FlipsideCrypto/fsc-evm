{% macro streamline_balances_native_daily_history_function_call() %}

{# Get variables #}
{% set vars = return_vars() %}

    {% set params = {
        "external_table": 'balances_native',
        "sql_limit": vars.BALANCES_SL_NATIVE_DAILY_HISTORY_SQL_LIMIT,
        "producer_batch_size": vars.BALANCES_SL_NATIVE_DAILY_HISTORY_PRODUCER_BATCH_SIZE,
        "worker_batch_size": vars.BALANCES_SL_NATIVE_DAILY_HISTORY_WORKER_BATCH_SIZE,
        "async_concurrent_requests": vars.BALANCES_SL_NATIVE_DAILY_HISTORY_ASYNC_CONCURRENT_REQUESTS,
        "sql_source": 'balances_native_daily_history'
    } %}

    {% set function_call_sql %}
    {{ fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = this.schema ~ "." ~ this.identifier,
        params = params
    ) }}
    {% endset %}

    {% do run_query(function_call_sql) %}

{% endmacro %}

{% macro streamline_balances_erc20_daily_history_function_call() %}

{# Get variables #}
{% set vars = return_vars() %}

    {# Get the maximum batch number from the to_do table #}
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
                CREATE OR REPLACE VIEW {{ this.schema }}.{{ batch_view_name }} AS
                SELECT * FROM {{ this.schema }}.{{ this.identifier }}
                WHERE batch = {{ batch_num }}
            {% endset %}
            {% do run_query(create_batch_view) %}
            {{ log("Created view: " ~ this.schema ~ "." ~ batch_view_name, info=True) }}
            
            {# Check if rows exist first #}
            {% set check_rows_query %}
                SELECT EXISTS(SELECT 1 FROM {{ this.schema }}.{{ batch_view_name }} LIMIT 1)
            {% endset %}
            
            {% set results = run_query(check_rows_query) %}
            {% set has_rows = results.columns[0].values()[0] %}
            
            {% if has_rows %}
                {# Update params with vars after testing #}
                {% set params = {
                "external_table": 'balances_erc20',
                "sql_limit": 20000,
                "producer_batch_size": 2000,
                "worker_batch_size": 200,
                "async_concurrent_requests": vars.BALANCES_SL_ERC20_DAILY_HISTORY_ASYNC_CONCURRENT_REQUESTS,
                "sql_source": batch_view_name
                } %}

                {% set function_call_sql %}
                {{ fsc_utils.if_data_call_function_v2(
                    func = 'streamline.udf_bulk_rest_api_v2',
                    target = this.schema ~ "." ~ batch_view_name,
                    params = params
                ) }}
                {% endset %}

                {% do run_query(function_call_sql) %}
                {{ log("Completed batch " ~ batch_num, info=True) }}
            {% else %}
                {{ log("No rows to process for batch " ~ batch_num, info=True) }}
            {% endif %}
            
            {# Clean up the temporary view #}
            {% set drop_batch_view %}
                DROP VIEW {{ this.schema }}.{{ batch_view_name }}
            {% endset %}
            {% do run_query(drop_batch_view) %}
        {% endfor %}
    {% endif %}

{% endmacro %}