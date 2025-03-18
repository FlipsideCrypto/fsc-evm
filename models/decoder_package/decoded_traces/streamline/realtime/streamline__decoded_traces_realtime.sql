{# Set variables #}
{%- set model_name = 'DECODED_TRACES' -%}
{%- set model_type = 'REALTIME' -%}

{%- set streamline_params = { 
    "external_table": get_var("DECODER_SL_DECODED_TRACES_REALTIME_EXTERNAL_TABLE", "decoded_traces"),
    "sql_limit": get_var("DECODER_SL_DECODED_TRACES_REALTIME_SQL_LIMIT", 10000000),
    "producer_batch_size": get_var("DECODER_SL_DECODED_TRACES_REALTIME_PRODUCER_BATCH_SIZE", 400000),
    "worker_batch_size": get_var("DECODER_SL_DECODED_TRACES_REALTIME_WORKER_BATCH_SIZE", 200000),
    "sql_source": "decoded_traces_realtime"
} -%}

{%- set testing_limit = get_var('DECODED_TRACES_REALTIME_TESTING_LIMIT', none) -%}

{# Log configuration details #}
{{ log_model_details( 
    params = streamline_params    
) }}

{# Set up dbt configuration #}
{{ config (
    materialized = "view",
    post_hook = [fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_decode_traces_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = {
            "external_table": streamline_params['external_table'],
            "sql_limit": streamline_params['sql_limit'],
            "producer_batch_size": streamline_params['producer_batch_size'],
            "worker_batch_size": streamline_params['worker_batch_size'],
            "sql_source": streamline_params['sql_source']
        }
    ),
    fsc_utils.if_data_call_wait()],
    tags = get_path_tags(model)
) }}

{# Main query starts here #}
{{ streamline_decoded_traces_requests(
    model_type = model_type.lower(),
    testing_limit = testing_limit
) }}