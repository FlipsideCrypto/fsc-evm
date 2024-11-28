{# Set variables #}
{%- set model_name = 'DECODED_TRACES' -%}
{%- set model_type = 'REALTIME' -%}

{%- set params = {
    "external_table": var("DECODED_TRACES_REALTIME_EXTERNAL_TABLE", "decoded_traces"),
    "sql_limit": var("DECODED_TRACES_REALTIME_SQL_LIMIT", 10000000),
    "producer_batch_size": var("DECODED_TRACES_REALTIME_PRODUCER_BATCH_SIZE", 400000),
    "worker_batch_size": var("DECODED_TRACES_REALTIME_WORKER_BATCH_SIZE", 200000),
    "sql_source": "decoded_traces_realtime"
} -%}

{%- set testing_limit = var('DECODED_TRACES_REALTIME_TESTING_LIMIT', none) -%}

{# Log configuration details #}
{{ log_streamline_details(
    model_name=model_name,
    model_type=model_type,
    testing_limit=testing_limit,
    streamline_params=params
) }}

{# Set up dbt configuration #}
{{ config (
    materialized = "view",
    tags = ['streamline_decoded_traces_realtime']
) }}

{# Main query starts here #}
{{ streamline_decoded_traces_requests(
    model_type = model_type.lower(),
    testing_limit = testing_limit
) }}
