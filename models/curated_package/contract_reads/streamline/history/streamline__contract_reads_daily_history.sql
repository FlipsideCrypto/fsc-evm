{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = "view",
    tags = ['streamline','contract_reads','history','phase_4']
) }}

SELECT 
    *
FROM
    {{ ref('streamline__contract_reads_daily_history_requests') }}

{# Streamline Function Call #}
{% if execute %}
    {% set params = {
        "external_table": 'contract_reads',
        "sql_limit": vars.CURATED_SL_CONTRACT_READS_DAILY_HISTORY_SQL_LIMIT,
        "producer_batch_size": vars.CURATED_SL_CONTRACT_READS_DAILY_HISTORY_PRODUCER_BATCH_SIZE,
        "worker_batch_size": vars.CURATED_SL_CONTRACT_READS_DAILY_HISTORY_WORKER_BATCH_SIZE,
        "async_concurrent_requests": vars.CURATED_SL_CONTRACT_READS_DAILY_HISTORY_ASYNC_CONCURRENT_REQUESTS,
        "sql_source": 'contract_reads_daily_history'
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
{% endif %}