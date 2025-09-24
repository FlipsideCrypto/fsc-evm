{% macro streamline_balances_history_batch_function_call() %}

{# Get variables #}
{% set vars = return_vars() %}

    {# Set params by model name #}
    {% if model.name == 'streamline__balances_erc20_daily_history' %}
        {% set external_table = 'balances_erc20' %}
        {% set sql_limit = vars.BALANCES_SL_ERC20_DAILY_HISTORY_SQL_LIMIT %}
        {% set producer_batch_size = vars.BALANCES_SL_ERC20_DAILY_HISTORY_PRODUCER_BATCH_SIZE %}
        {% set worker_batch_size = vars.BALANCES_SL_ERC20_DAILY_HISTORY_WORKER_BATCH_SIZE %}
        {% set async_concurrent_requests = vars.BALANCES_SL_ERC20_DAILY_HISTORY_ASYNC_CONCURRENT_REQUESTS %}
    {% elif model.name == 'streamline__balances_native_daily_history' %}
        {% set external_table = 'balances_native' %}
        {% set sql_limit = vars.BALANCES_SL_NATIVE_DAILY_HISTORY_SQL_LIMIT %}
        {% set producer_batch_size = vars.BALANCES_SL_NATIVE_DAILY_HISTORY_PRODUCER_BATCH_SIZE %}
        {% set worker_batch_size = vars.BALANCES_SL_NATIVE_DAILY_HISTORY_WORKER_BATCH_SIZE %}
        {% set async_concurrent_requests = vars.BALANCES_SL_NATIVE_DAILY_HISTORY_ASYNC_CONCURRENT_REQUESTS %}
    {% endif %}

    {# Get the maximum batch number from the history table #}
    {% set max_batch_query %}
        SELECT COALESCE(MAX(batch), -1) AS max_batch 
        FROM {{ this.schema }}.{{ this.identifier }}
    {% endset %}
    
    {% if execute %}
        {% set results = run_query(max_batch_query) %}
        {% set max_batch = results.columns[0].values()[0] %}
        
        {# Create a single view that unions all batches #}
        {% set batch_view_name = this.identifier ~ '_batch' %}
        {% set create_batch_view %}
            CREATE OR REPLACE VIEW streamline.{{ batch_view_name }} AS
            {% for batch_num in range(max_batch + 1) %}
                {% if batch_num > 0 %}UNION ALL{% endif %}
                SELECT
                    *
                FROM
                    {{ this.schema }}.{{ this.identifier }}
                WHERE batch = {{ batch_num }}
            {% endfor %}
        {% endset %}
        {% do run_query(create_batch_view) %}
        {{ log("Created batch view: streamline." ~ batch_view_name, info=True) }}
        
        {# Call the streamline function on the batch view #}
        {% set params = {
            "external_table": external_table,
            "sql_limit": sql_limit,
            "producer_batch_size": producer_batch_size,
            "worker_batch_size": worker_batch_size,
            "async_concurrent_requests": async_concurrent_requests,
            "sql_source": batch_view_name
        } %}

        {% set function_call_sql %}
        {{ fsc_utils.if_data_call_function_v2(
            func = 'streamline.udf_bulk_rest_api_v2',
            target = "streamline." ~ batch_view_name,
            params = params
        ) }}
        {% endset %}

        {% do run_query(function_call_sql) %}
        {{ log("Completed processing batch view", info=True) }}
    {% endif %}

{% endmacro %}