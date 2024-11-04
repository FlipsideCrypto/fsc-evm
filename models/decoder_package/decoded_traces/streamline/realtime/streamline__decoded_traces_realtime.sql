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

{%- set default_vars = set_default_variables_streamline_decoder(model_name, model_type) -%}


{%- set testing_limit = default_vars['testing_limit'] -%}

{# Log configuration details #}
{{ log_streamline_details(
    model_name=model_name,
    model_type=model_type,
    testing_limit=testing_limit,
    streamline_params=streamline_params
) }}

{# Set up dbt configuration #}
{{ config (
    materialized = "view",
    post_hook = [fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_decode_traces_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = {
            "external_table": params['external_table'],
            "sql_limit": params['sql_limit'],
            "producer_batch_size": params['producer_batch_size'],
            "worker_batch_size": params['worker_batch_size'],
            "sql_source": params['sql_source']
        }
    ),
    fsc_utils.if_data_call_wait()],
    tags = ['streamline_decoded_traces_realtime']
) }}

{# Main query starts here #}
{{ streamline_decoded_traces_requests(
    model_type = model_type.lower(),
    testing_limit = testing_limit
) }}