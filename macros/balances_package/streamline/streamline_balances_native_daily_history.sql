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
    {{ log("Streamline function call: " ~ function_call_sql, info=true) }}

{% endmacro %}